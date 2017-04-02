import re
from pyspark import SparkContext, SparkConf
from user import LogEntry
from sklearn.feature_extraction.text import CountVectorizer

import os
import sys
import re
import sh

from nltk.corpus import stopwords
from bs4 import BeautifulSoup  
from nltk.stem import PorterStemmer

ALL_STOPWORDS = set(stopwords.words('english'))
porter = PorterStemmer()

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

    return " ".join(body)

def applyStoppingandStemming(line):
        line = line.lower().strip()
        example1 = BeautifulSoup(line)
        line = re.sub("[^a-zA-Z]", " ", example1.get_text() )
        line = line.lower() 
        words = line.split() 

        stops = set(ALL_STOPWORDS)  
        meaningful_words = [w for w in words if not w in stops]
        email_data = " ".join( meaningful_words )

        hk_line = ""
        for word in email_data.split(" "):
            wd = porter.stem(word)
            hk_line = hk_line + wd + " "

        email_data = hk_line.strip()

	return email_data



conf = SparkConf().setAppName("Bag_of_words")
sc = SparkContext(conf=conf)
all_emails = list()
baseRdd = sc.wholeTextFiles("hdfs:///maildir/*/all_documents/*")
c = baseRdd.map(lambda (file_name, file_content): (extract_username(file_name), preprocess_file_content(file_content)))
email_body = c.map(lambda c: c[1]).cache()
processed_body = email_body.map(applyStoppingandStemming)
all_emails = processed_body.collect()
countVector = CountVectorizer()
featuresList = countVector.fit_transform(all_emails)
featuresList = featuresList.toarray()
vocab = countVector.get_feature_names()
vocabList = open("/home/hadoop/output_assignment2/vocabList.txt",'w+')
j = 0
for vocab_word in vocab:
    vocabList.write(vocab_word)
    if not (j==len(vocab)):
        vocabList.write("\n")
    j = j + 1

lda_data = open("/home/hadoop/output_assignment2/Bag_of_words.txt",'w+')
for i in range(len(featuresList)):
    for j in range(len(featuresList[i])):
        c = featuresList[i][j]
        lda_data.write(str(c))
        if(j!=(len(featuresList[i])-1)):
            lda_data.write(" ")

    if(i!=(len(featuresList)-1)):
        lda_data.write("\n")


