SET default_parallel $reducerNum ;
SET job.name 'BSBM Q6 ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
rdfs_label = LOAD '$inputData/rdfs_label' USING PigStorage('\t') AS (s,o) ;


-- BGP
f0 = FILTER rdfs_label BY o matches '.*$word1.*' ; 
t0 = FOREACH f0 GENERATE s AS product, o AS label ;
f1 = FILTER rdf_type BY o == 'bsbm:Product' ; 
t1 = FOREACH f1 GENERATE s AS product ;
BGP1 = JOIN t0 BY product, t1 BY product ;
BGP1 = FOREACH BGP1 GENERATE t0::product AS product, t0::label AS label ;

-- store results into output
STORE BGP1 INTO '$outputData' USING PigStorage('\t') ;
