import re
from pyspark import SparkContext, SparkConf
from user import LogEntry

import os
import sys
import re
import sh

from nltk.corpus import stopwords
from bs4 import BeautifulSoup  
from nltk.stem import PorterStemmer

STOPWORDS = set(stopwords.words('english'))
porter = PorterStemmer()

# Reference taken from the Code shared by TA http://web.cs.dal.ca/~gercek/spark/script/assignment_sample.py

def extract_username(file_name):
    m = re.match(r'^.+/(.+)/all_documents/([^/]+)$', file_name)
    if m is not None:
        return m.groups()
    return ()


def preprocess_file_content(file_content):
    body = []
    headers = []
    headers_done = False

    for line in file_content.splitlines():
        line = line.strip()
        if headers_done is False:
            if line == '':
                headers_done = True
            headers.append(line)
        else:
            body.append(line)

    return (parse_headers(headers), " ".join(body))


def parse_headers(headers):
    parsed_headers = {}
    last_key = ""
    from_email = ''
    name = ''
    num_emails = 0
    num_words = 0
    subject = ''
    to_e = list()
    to_mail = ''
    for header in headers:
        if ':' in header:
            tmp = header.strip().split(":")
            key = tmp[0]
            value = ":".join(tmp[1:])
            if(key=='X-From'):
                name = value
            elif(key=='From'):
                from_email = value
            elif(key=='To'):
                
                if(';' in value):
		        to_email = value.strip().split(";")
		        if (len(to_email) != 0):
		            for toemail in to_email:
				final_emails = list()
		                final_emails = toemail.strip().split(" ");
		                if ('@' in final_emails[0]):
				    fline = re.sub(r'[?|$|"\'|!<>]',r'',final_emails[0])
				    fline = fline.split(" ")
		                    ln=''
                                    i = 0
				    for email in fline:
                                        to_e.append(email.strip())
                                        if(i>=(len(fline)-1)):
				            ln = from_email+"$$"+email+"##"
                                        else:
					    ln = from_email+"$$"+email+"##"
					ln=ln.strip()
                                        i = i + 1
				    to_mail = to_mail + ln
			to_mail=to_mail[:-2]
		else:
		        to_email = value.strip().split(",")
		        if (len(to_email) != 0):
		            for toemail in to_email:
				final_emails = list()
		                final_emails = toemail.strip().split(" ");
		                if ('@' in final_emails[0]):
				    fline = re.sub(r'[?|$|"\'|!<>]',r'',final_emails[0])
				    fline = fline.split(" ")
		                    ln = ""
                                    i = 0
				    for email in fline:
                                        to_e.append(email.strip())
                                       	if(i>=(len(fline)-1)):
                                            ln = from_email+"$$"+email+"##"
                                       	else:
                                            ln = from_email+"$$"+email+"##"
                                       	i = i + 1
					ln=ln.strip()
				    to_mail = to_mail + ln
                        to_mail=to_mail[:-2]
            elif(key=='Subject'):
                num_emails = num_emails + 1
                subject = value
           
            last_key = key
            parsed_headers[key] = value
        else:
            parsed_headers[last_key] += value
    
    return LogEntry(from_email.strip(), name.strip(), num_emails, num_words, to_e, to_mail, subject)

def applyStoppingandStemming(line):
        line = line.lower().strip()
        example1 = BeautifulSoup(line)
        line = re.sub("[^a-zA-Z]", " ", example1.get_text() )
        line = line.lower() 
        words = line.split() 

        stops = set(STOPWORDS)  
        meaningful_words = [w for w in words if not w in stops]
        email_data = " ".join( meaningful_words )
        words = email_data.split()

        hk_line = ""
       	for word in email_data.split(" "):
       	    wd = porter.stem(word)
       	    hk_line = hk_line + wd + " "

        email_data = hk_line.strip()

        return email_data

def getEmail(c):
    return c.email

def getName(c):
    return c.name

def getTo(c):
    return c.to

def getToEmail(c):
    return c.to_mail

file_path = sys.argv[1]
output_path = sys.argv[2]
conf = SparkConf().setAppName("Assignment Task 1 and Task 2")
sc = SparkContext(conf=conf)
baseRdd = sc.wholeTextFiles(file_path+"/*/all_documents/*")
c = baseRdd.map(lambda (file_name, file_content): (extract_username(file_name), preprocess_file_content(file_content))).cache()

email_headers = c.map(lambda c: c[1][0]).cache()

users = c.map(lambda c: getToEmail(c[1][0])).cache()
users.saveAsTextFile(output_path+"/users")

# Preprocessing - removing header info, stop words, attachment info and apply stemming
email_body = c.map(lambda c: c[1][1]).cache()
processed_body = email_body.map(applyStoppingandStemming).cache()
processed_body.saveAsTextFile(output_path+"/s01_DataAfterStoppingAndStemming")
finalRDD = c.map(lambda c:((getEmail(c[1][0])), (getName(c[1][0]), 1, len(applyStoppingandStemming(c[1][1]).split()), getTo(c[1][0])))).reduceByKey(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2], a[3] + b[3]))


# Data Discovery part
final_result = finalRDD.map(lambda x: (x[0],(x[1][0], x[1][1], x[1][2]/(x[1][1]+1), len(set(x[1][3])))))
final_result.saveAsTextFile(output_path+"/s02_DataDiscovery_stastistical_summary")
to = email_headers.map(lambda entry: (entry.email, entry.to_mail))
graph_part = to.flatMap(lambda x: '' if (len(x)==0) else (x[1].split("##"))  ).map(lambda x:(('',''),'') if(len(x)==0) else ((x.split('$$')[0],x.split('$$')[1]),1))


#Graph representation - both sent and received edges
final_graph_results = graph_part.map(lambda x:(('',''),'') if(len(x)==0) else x).reduceByKey(lambda a, b : a+b).cache()
final_graph = final_graph_results.map(lambda x : (x[0][0], (x[0][1],x[1])))
final_graph.saveAsTextFile(output_path+"/s03_Directional_Graph_sent_Edges")
final_graph2 = final_graph_results.map(lambda x : (x[0][1], (x[0][0],x[1])))
final_graph2.saveAsTextFile(output_path+"/s04_Directional_Graph_received_Edges")


