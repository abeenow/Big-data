-- Delete the file if already present
rmf $output_location

-- Start of script
A = LOAD '$business_file' USING PigStorage('^');
B = LOAD '$review_file' USING PigStorage('^');
C = COGROUP A BY $2, B BY $2;
D = LIMIT C 6;

STORE D INTO '$output_location' USING PigStorage('\t');
