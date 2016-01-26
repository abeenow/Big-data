The following readme file summarizes the list of steps that must be followed for deploying the pig script on my local machine.
Since the cluster was not able to excute Map reduce jobs that were submitted by Hive, I had to deploy the code on my local machine

* Question 5
   Command:
     nohup time hive -f Q5.hive -d review_file=../../Data/review2.csv -d business_file=../../Data/business2.csv
   Arguments:
    -f : The hive script file
    -d : The script uses two variables, that needs assignment during script execution they are business_file and review_file and output_location so the -d argument helps in assigning the required values to these variables
    Note that the variables used inside the script actually corresponds to the local location in which the data files are located. Also note that all the tables are created inside the axs143632(NET ID) schema.

* Question 6
   Command:
     nohup time hive -f Q6.hive
   Arguments:
    -f : The hive script file
    
* Question 7
  Command:
     nohup time hive -f Q7.hive -d business_data_dir=../../Data 
  Arguments:
    -f : The hive script file
    -d : The script uses one variable, that needs assignment during script execution it is business_data_dir so the -d argument helps in assigning the required values to these variables
    Note that the variables used inside the script actually corresponds to the local location in which the data files are located. Also note that all the tables are created inside the axs143632(NET ID) schema.
    
* Question 8
   Command:
     nohup time hive -f Q8.hive
   Arguments:
    -f : The hive script file

* Question 9
   Command:
     nohup time hive -f Q9.hive
   Arguments:
    -f : The hive script file
	Note that the UDF.jar conataing the FORMAT_CAT.class is added to HIVE_CLASSPATH inside the hive script and the Format_cat temporary function is created inside the script.



NOTE:
nohup command is used for redirecting all the output to the nohup.out file instead of displaying it on the terminal screen.
time command is used for finding the total time consumed for executing each of the scripts
