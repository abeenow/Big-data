a. Start spark-shell in local mode using all the processor cores on my system
	To find the number of cores in my system, I used nproc command
	$> nproc
	2
	My machine is a dual core machine so to start spark-shell in local mode using all cores, I used the following command:
		pyspark --master local --name TopTenBusiness --driver-cores 2
		
		
b. Start spark-shell in YARN mode using Cs6360 spark cluster.
   This spark cluster consist 6 hadoop machine nodes.
   Using the following parameters Rerun your scala script from question 2a.
   
	Set executor memory =2G
	executor cores = 6.
	num-executors = 6

		spark-shell --master yarn-client --executor-memory 2G --executor-cores 6 --num-executors 6




Comments on the performance of the code for the second question:

	The same code was executed on the UTD cluster and my local machine, the code was able to run way faster on the cluter becaues of the following reasons:
		* More slave nodes(Mine was a standalone instance of spark)
		* More amount of memeory
		* More amount of processing power
		* HDFS running in the back-end
		
