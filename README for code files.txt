README for the code files

hive-commands.txt consists of hive commands that creates tables of all the cleaned and profiled datasets that were emitted from the Map Reduce jobs and does mapping of 311 calls for service and crimes to street segments using the Spatial Analysis for Hive library.

spark-shell-commands.txt consists of Spark shell commands that ingest the output from Hive, which is two files, one of the 311 calls for service and the other of crimes. Then data related to the years under study are extracted and this extracted data is fed to the Generalized Linear Regression function to train two Negative Binomial Regression Models. The results of the analysis are then printed.

All the input data for the Map Reduce jobs, the original datasets, is found in hdfs:///user/ph1130/rbdaproject/data
The output of the Map Reduce jobs was ingested by hive and can be found in the hive data warehouse.
All the data ingested by Spark, i.e, output of Hive, can be found in hdfs:///user/ph1130/rbdaproject/