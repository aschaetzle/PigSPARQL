SET default_parallel $reducerNum ;
SET job.name 'BSBM Q1 replicated ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
bsbm_productFeature = LOAD '$inputData/bsbm_productFeature' USING PigStorage('\t') AS (s,o) ;
rdfs_label = LOAD '$inputData/rdfs_label' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric1 = LOAD '$inputData/bsbm_productPropertyNumeric1' USING PigStorage('\t') AS (s,o) ;


-- BGP
t1 = FOREACH rdfs_label GENERATE s AS product, o AS label ;

f2 = FILTER rdf_type BY o == '$ProductType' ;
t2 = FOREACH f2 GENERATE s AS product ;

f3 = FILTER bsbm_productFeature BY o == '$ProductFeature1' ;
t3 = FOREACH f3 GENERATE s AS product ;

f4 = FILTER bsbm_productFeature BY o == '$ProductFeature2' ;
t4 = FOREACH f4 GENERATE s AS product ;

f5 = FILTER bsbm_productPropertyNumeric1 BY (o > $x) ;
t5 = FOREACH f5 GENERATE s AS product ;

BGP1 = JOIN t1 BY product, t5 BY product, t3 BY product, t2 BY product, t4 BY product USING 'replicated' ;
BGP1 = FOREACH BGP1 GENERATE t1::product AS product, t1::label AS label ;


-- SM_Distinct
SM_Distinct = DISTINCT BGP1 ;

-- SM_Order
SM_Order = ORDER SM_Distinct BY label ;

-- SM_Slice
SM_Slice = LIMIT SM_Order 10 ;

-- store results into output
STORE SM_Slice INTO '$outputData' USING PigStorage('\t') ;
