-- Delete the file if already present
rmf $output_location

-- Start of script
A = LOAD '$business_file' USING PigStorage('^');
B = LOAD '$review_file' USING PigStorage('^');
C = COGROUP A BY $2 INNER, B BY $2 INNER;
D = FOREACH C GENERATE FLATTEN(A),FLATTEN(B);
E = LIMIT D 6;

STORE E INTO '$output_location' USING PigStorage('\t');
