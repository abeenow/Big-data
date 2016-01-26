-- Delete the file if already present
rmf $output_location

-- Register the jar file (Assuming that the jar is in $PWD)
REGISTER UDF.jar;
-- Start of script
A = LOAD '$business_file' USING PigStorage('^');
B = FOREACH A GENERATE $2 AS business_id,$3 AS full_address,FORMAT_CAT((chararray)$10) AS categories;
C = LIMIT B 10;

STORE C INTO '$output_location' USING PigStorage('\t');
