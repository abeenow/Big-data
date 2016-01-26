-- Delete the file if already present
rmf $output_location

-- Start of script
A = LOAD '$business_file' USING PigStorage('^');
B = FOREACH A GENERATE $2 AS business_id,$3 AS full_address,$10 AS categories;

C = LOAD '$review_file' USING PigStorage('^');
D = FOREACH C GENERATE $2 AS business_id,$20 AS rating;
E = GROUP D BY business_id;
F = FOREACH E GENERATE FLATTEN(group) AS business_id,AVG(D.rating) AS avg;

G = JOIN B BY business_id,F by business_id;
H = FOREACH G GENERATE $0 AS business_id,$1 AS full_address,$2 AS categories,$4 AS avg;
I = ORDER H BY avg DESC;
J = LIMIT I 10;

STORE J INTO '$output_location' USING PigStorage('\t');
