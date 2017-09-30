# Hadoop-TFIDF-Search
Take a database corpus, generate a TFIDF score of words and perform a search using MapReduce in Hadoop.

Files included - 

Source code folder:
	1. DocWordCount.java
	2. TermFrequency.java
	3. TFIDF.java
	4. Search.java
Output folder:
	1. DocWordCount.out
	2. TermFrequency.out
	3. TFIDF.out
	4. query1.out
	5. query2.out

----------------------------------------
Execution instructions:
----------------------------------------
1. Make sure output directory is not already present in HDFS. If it is present,
delete it using - 
hadoop fs -rm -r path_to_delete

Assumption - 
All java files are in current directory.
"Build" directory present in current directory.

I am using cloudera VM. My jar files are in /home/cloudera/java_file.jar

Starting execution - 

1. Put canterbury corpus unzipped to HDFS file system using - 
hdfs dfs -put /home/cloudera/Downloads/canterbury /canterbury
This will put canterbury folder in HDFS / path.

------------------------------------------

DocWordCount.java

1. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint
jar -cvf docwordcount.jar -C build/ .
2. Alternatively, we can export jar directly using Eclipse -> Export
3. Run the program - 
hadoop jar /home/cloudera/docwordcount.jar DocWordCount /canterbury /output_dwc_canterbury
4. Get the output file to local system using following command - 
hdfs dfs -get /output_dwc_canterbury/part-r-00000 /desired_local_file_system_path


TermFrequency.java

1. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint
jar -cvf termfrequency.jar -C build/ .
2. Alternatively, we can export jar directly using Eclipse -> Export
3. Run the program - 
hadoop jar /home/cloudera/termfrequency.jar TermFrequency /canterbury /output_tf_canterbury
4. Get the output file to local system using following command - 
hdfs dfs -get /output_tf_canterbury/part-r-00000 /desired_local_file_system_path

TFIDF.java (Here, we also have to compile TermFrequency.java for chaining two MR jobs)

1. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:. *.java -d build -Xlint
jar -cvf tfidf.jar -C build/ .
2. Alternatively, we can export jar directly using Eclipse -> Export
3. Run the program - 
hadoop jar /home/cloudera/tfidf.jar TFIDF /canterbury /output_tf_intermediate_canterbury /output_tfidf_canterbury
4. Get the output file to local system using following command - 
hdfs dfs -get /output_tfidf_canterbury/part-r-00000 /desired_local_file_system_path

Search.java

1. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d build -Xlint
jar -cvf search.jar -C build/ .
2. Alternatively, we can export jar directly using Eclipse -> Export
3. Run the program - 
hadoop jar /home/cloudera/search.jar Search /output_tfidf_canterbury /output_query_one "search query"
4. Get the output file to local system using following command - 
hdfs dfs -get /output_query_one/part-r-00000 /desired_local_file_system_path

In our case - 
Search query are "computer science" and "data analysis"
So,
hadoop jar /home/cloudera/search.jar Search /output_tfidf_canterbury /output_query_one "computer science"
hadoop jar /home/cloudera/search.jar Search /output_tfidf_canterbury /output_query_two  "data analysis"

And, to view output in hdfs only - 
hdfs dfs -cat /output_query_one/part-r-00000
OR
we can anytime get in local file system and read it.

Important note - It is assumed in the assignment that "Computer Science" query should also 
fetch results same as "computer science" as that will be more user friendly and makes more sense.

Compile may give warnings, which is ok, the program will work like a breeze without failure.
If we don't want to see those warnings, we can suppress warnings at any point. (It doesn't make a difference on performance)

- If loaded in eclipse and exporting project, please add the external jar files in the Eclipse path. (by right clicking project -> properties -> Build Path -> Add External Jars -> Add hadoop jars) 

