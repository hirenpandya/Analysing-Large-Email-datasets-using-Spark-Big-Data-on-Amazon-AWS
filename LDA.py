#Reference - https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/latent_dirichlet_allocation_example.py
from __future__ import print_function
import sys
import os
import re

from sklearn.feature_extraction.text import CountVectorizer
from pyspark import SparkContext
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vectors


if __name__ == "__main__":
    sc = SparkContext(appName="LatentDirichletAllocationExample")  # SparkContext
    filespath = sys.argv[1]
    k_val = sys.argv[2]
    output_folder = '/home/hadoop/output_assignment2'

    vocab = list()
    with open(output_folder+"/vocabList.txt") as f:
        for line in f:
            line = line.strip()
            vocab.append(line)

    lda_dt8 = open(output_folder+"/LDA_clustering_output.txt",'w+')


    data = sc.textFile(filespath)
    parsedData = data.map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))

    corpus = parsedData.zipWithIndex().map(lambda x: [x[1], x[0]]).cache()


    ldaModel = LDA.train(corpus, k=k_val)


    print("Learned topics (as distributions over vocab of " + str(ldaModel.vocabSize())
          + " words):")
    topics = ldaModel.describeTopics(k_val)

    print("The topics described by their top-weighted terms:")
    lda_dt8.write("The topics described by their top-weighted terms: \n")

    lda_dt8.write("Index Terms for each topic \n")
    j=1
    for topic in topics:
        topic_words = ""
        t = topic[0]
        for index in t:
            topic_words = topic_words + str(index) + ", "

        lda_dt8.write("Topic "+str(j)+" : " + topic_words+"\n\n")
        j = j + 1

    lda_dt8.write("\n\nTopics in words \n")

    i=1
    topic_array = list()
    for topic in topics:
        topic_words = ""
        t = topic[0]
        for index in t:
            topic_words = topic_words + vocab[index] + " "

        topic_array.append(topic_words)
        print ("Topic "+str(i)+" : " + topic_words+"\n")
        lda_dt8.write("Topic "+str(i)+" : " + topic_words+"\n\n")	
        i = i + 1

    topic_matrix = ldaModel.topicsMatrix()

    topic_by_email = open(output_folder+"/email_to_topic.txt",'w+')

    with open(output_folder+"/Bag_of_words.txt") as f:
        z = 1
        for line in f:
            final_topic = 0
            sum_pre = 0
            qw = line.split()
            for topic in range(k_val):
                sum_result = 0
    	        for word in range(0, ldaModel.vocabSize()):
                    w = int(qw[word]) * topic_matrix[word][topic]
		    sum_result = sum_result + w

                if(sum_result>sum_pre):
                    final_topic = topic

                sum_pre = sum_result
            
            topic_by_email.write(str(z)+"-->"+topic_array[final_topic-1]+"\n")
            z = z + 1

    k = "/output_assignment2/LDAModel"
    ldaModel.save(sc, k)
    sameModel = LDAModel.load(sc, k)

    sc.stop()
