
from __future__ import print_function
import sys
import os
import re
import sh

from pyspark import SparkContext
from sklearn.feature_extraction.text import CountVectorizer

if __name__ == "__main__":

    sc = SparkContext(appName="Graph for likely communication")
    temp_entries = sc.emptyRDD() 
    finalRDD = sc.emptyRDD()
    output_folder = '/home/hadoop/output_assignment2/'
    topic_email = list()

    with open(output_folder+"/topic_by_email.txt") as f:
        h = 0;
        for line in f:
            w = line.split("-->")
            topic_email.append(w[1].strip())
    
    hdfsdir = '/output_assignment2/users'
    e_files = [ line.rsplit(None,1)[-1] for line in sh.hdfs('dfs','-ls',hdfsdir).split('\n') if len(line.rsplit(None,1))][1:]

    for e_file in e_files:
        if(("SUCCESS" in e_file) or ("crc" in e_file)):
            l=""
        else:
	    emailfile = sc.textFile(e_file)
            finalRDD = temp_entries.union(emailfile)
            temp_entries = finalRDD 

    user_graph = finalRDD.flatMap(lambda x: '' if (len(x)==0) else (x.split("##"))  ).map(lambda x:(('',''),'') if(len(x)==0) else (x.split('$$')[0],x.split('$$')[1]))
    
    final_arr = list()
    final_arr = user_graph.collect()
    lns = ''   
    k = 0
    graph_data = open("/home/hadoop/output_assignment2/user_communication_graph.txt",'w+')

    for line in final_arr:
        topic = topic_email[k]
        words = topic.split()
        f_topic = ''
        j=0
        for word in words:
            if(j<8):
                f_topic = f_topic + word + " "
            j = j + 1

        ln = "("+str(line)+",("+f_topic+"))"
        lns = lns + ln + "\n"
        k = k + 1

    print (lns)
    graph_data.write(lns)

