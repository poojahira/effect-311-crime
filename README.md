# Overview

New York City has experienced a remarkable drop in its crime rate over the past three decades. The credit for this often goes to its adoption of the broken windows theory of policing, though several doubts have been expressed about the efficacy of the theory and its consequences for minorities. I analyze the effects of successfully resolved 311 calls for service related to neighborhood disorder on the incidence of crime at the street segment level, as an evaluation of the soundness of the broken windows theory. The project involved downloading crime, 311 calls for service and NYC Centerline data from https://opendata.cityofnewyork.us/. The datasets were profiled and cleaned using MapReduce. Then the data was moved to Hive for further refinement and for the mapping of crimes and 311 calls to street segments using the Spatial Framework for Hadoop library. Subsequently the data was moved to Spark where two Negative Binomial Regression models were trained and analyzed.

mapreduce folder consists of MapReduce jobs written to clean and profile the three datasets. 

hive-commands.txt consists of hive commands that creates tables of all the cleaned and profiled datasets that were emitted from the Map Reduce jobs and does mapping of 311 calls for service and crimes to street segments using the Spatial Analysis for Hive library.

spark-shell-commands.txt consists of Spark shell commands that ingest the output from Hive, which is two files, one of the 311 calls for service and the other of crimes. Then data related to the years under study are extracted and this extracted data is fed to the Generalized Linear Regression function to train two Negative Binomial Regression Models. The results of the analysis are then printed.

All data available at https://opendata.cityofnewyork.us/. Please see pdf of the study for specific links. 
