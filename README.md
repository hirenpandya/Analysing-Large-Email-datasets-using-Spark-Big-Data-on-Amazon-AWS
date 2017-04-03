# hiren-pandya

This file contains the installation and execution steps for this Project. I have used python’s pyspark module for creating Spark applications

Installation Instructions

Required Python vesion
1. Python 2.7.12 

Required python modules:
1. pyspark
2. py4j 

Third party modules:
1. nltk
2. bs4
3. sh
4. sklear-

1. preprocessingDataDiscovery.py (For Data preprocessing and data discovery and graph representation)
2. bag_of_words.py (for creating bag of words over all emails which will be input for LDA and Kmeans)
3. Kmeans.py  (for running K-means algorithm)
4. LDA.py (for running LDA algorithm)

Configure Spark cluster on Amazon AWS :
Please follow the steps shared by TA. Please also execute those three commands which copy maildir dataset on AWS hadoop file system.

After this, please create one folder on AWS called ‘source_code’. It would be /home/hadoop/source_code. Copy  all the files from this link or attached folder Source_code/ to the /home/hadoop/source_code folder. Now Go to this source_code folder using below commands.

cd /home/hadoop/source_code


Please follow below instructions to run all spark applications. Execution Instructions are as follows: 

1. Preprocessing and Data Discovery (Task 1 and Task 2): It has been implemented in the script preprocessingDataDiscovery.py. This file needs  
   one class file userObject.py. The command is as follow :  

   spark-submit --py-files userObject.py preprocessingDataDiscovery.py
 
   It will save the following four folders in the hdfs. The output directory in hdfs is output_assignmeent2.
        s01_DataAfterStoppingAndStemming
	s02_DataDiscovery_stastistical_summary 
	s03_Directional_Graph_sent_Edges
	s04_Directional_Graph_received_Edges
	

2.  Creating Bag of words : It has been implemented in the script bag_of_words.py. The command is as follows : 

    spark-submit bag_of_words.py

    It will create bag of words Bag_of_words.txt and vocabList.txt files in the output directory. We need to copy these files to hdfs folder / 
    output_assignment2.


3. Running K-means Algorithm

   spark-submit Kmeans.py  <INPUT_PATH> <K_VALUE>
   The output file is Kmeans_clustering_output.txt.


4. Running LDA Algorithm

   spark-submit LDA.py  <INPUT_PATH> <NO_OF_TOPICS>
   The output file is LDA_clustering_output.txt
   The LDAModel is saved in hdfs output directory (/output_assignment2/).

