Since I preferred to work in python, I wanted to express the following assumptions/considerations that I have made during the assignment execution

Q1. The first question is written in python and will be executed in my local cluster while showing the demo
Q2. The first part of the question is done using python, the second part however demands the execution of script in the cluster in yarn mode. Since the cluster is not capable of running python code in Yarn mode, I have written the scala code separately for this question
Q3. 1. For the Mahout execution, I have used the cluster for running the item-similarity algorithm and I have copied the file producerd to my local cluster, in order to excute the next two questions using python.
    2. I have used python for finding out the business_id's for which the user has given a rating as 4
	3. Similarly, using oython, I have joined the output of Q1 and Q2 to produce the final output.


Commands for Execution:

Q1. spark-submit --master yarn --name FilterLocation Q1.py -i /business3.csv -o /run1 -l Palo
	Q1.py -i <inputfile> -o <outputfile> -l <Location Filter>
	
Q2. a. This will be excuted in the python shell	in my local machine

	b. This will be executed in the scala shell in the UTD cluster

Q3. a. Refer Q3_1.txt

	b. spark-submit --master yarn --name FilterBusinessId ../Q3_2.py -i /review.csv -o /run1 -u sCiWf7Kai0t-x25CAz2DEQ
	Q3_2.py -i <inputfile> -o <outputfile> -u <User_Id>
	
	c. spark-submit --master yarn --name FindSimilairBusiness ../Q3_3.py -b /business_id.txt -m /file.txt -o /Matrix
	Q3_3.py -b <business_ids> -m <matrix> -o <outputfile>

