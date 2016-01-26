The following readme file summarizes the list of steps that must be followed for deploying the pig scripts on the UTD cluster.
* Question 1
 Command:
   nohup time pig -f $HOME/bigdata/HW2/pig/Q1.pig -p business_file=/SUMMER2015_HW2_PIG/business2.csv -p review_file=/SUMMER2015_HW2_PIG/review2.csv -p output_location=/$USER/pig1
   Arguments:
    -f : The pig script file
    -p : The script uses three variables, that needs assignment during script execution they are business_file, review_file and output_location so the -p argument helps in assigning the required values to these variables
    Note that the output location will be deleted before every run by using the rmf command inside the pig script. Also note that the variables used inside the script actually corresponds to the hdfs location in which the files are located.

* Question 2
 Command:
   nohup time pig -f $HOME/bigdata/HW2/pig/Q2.pig -p business_file=/SUMMER2015_HW2_PIG/business2.csv -p review_file=/SUMMER2015_HW2_PIG/review2.csv -p output_location=/$USER/pig2
   Arguments:
    -f : The pig script file
    -p : The script uses three variables, that needs assignment during script execution they are business_file, review_file and output_location so the -p argument helps in assigning the required values to these variables
    Note that the output location will be deleted before every run by using the rmf command inside the pig script. Also note that the variables used inside the script actually corresponds to the hdfs location in which the files are located.

* Question 3
 Command:
   nohup time pig -f $HOME/bigdata/HW2/pig/Q3.pig -p business_file=/SUMMER2015_HW2_PIG/business2.csv -p review_file=/SUMMER2015_HW2_PIG/review2.csv -p output_location=/$USER/pig3
   Arguments:
    -f : The pig script file
    -p : The script uses three variables, that needs assignment during script execution they are business_file, review_file and output_location so the -p argument helps in assigning the required values to these variables
    Note that the output location will be deleted before every run by using the rmf command inside the pig script. Also note that the variables used inside the script actually corresponds to the hdfs location in which the files are located.

* Question 4
 Command:
   nohup time pig -f $HOME/bigdata/HW2/pig/Q4.pig -p business_file=/SUMMER2015_HW2_PIG/business2.csv -p output_location=/$USER/pig4
   Arguments:
    -f : The pig script file
    -p : The script uses two variables, that needs assignment during script execution they are business_file and output_location so the -p argument helps in assigning the required values to these variables
    The UDF.jar file containing the Format_cat.class pig udf is registered before the script starts to run.
    Note that the output location will be deleted before every run by using the rmf command inside the pig script. Also note that the variables used inside the script actually corresponds to the hdfs location in which the files are located.
    
NOTE:
nohup command is used for redirecting all the output to the nohup.out file instead of displaying it on the terminal screen.
time command is used for finding the total time consumed for executing each of the scripts    
