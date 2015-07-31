SET default_parallel $reducerNum ;
SET job.name 'BSBM Q3 ON $inputData' ;

-- load input data
bsbm_productFeature = LOAD '$inputData/bsbm_productFeature' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric1 = LOAD '$inputData/bsbm_productPropertyNumeric1' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric3 = LOAD '$inputData/bsbm_productPropertyNumeric3' USING PigStorage('\t') AS (s,o) ;
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
rdfs_label = LOAD '$inputData/rdfs_label' USING PigStorage('\t') AS (s,o) ;


-- BGP
t0 = FOREACH rdfs_label GENERATE s AS product, o AS label ;
f1 = FILTER rdf_type BY o == '$ProductType' ;
t1 = FOREACH f1 GENERATE s AS product ;
f2 = FILTER bsbm_productFeature BY o == '$ProductFeature1' ;
t2 = FOREACH f2 GENERATE s AS product ;
f3 = FILTER bsbm_productPropertyNumeric1 BY (o > $x);
t3 = FOREACH f3 GENERATE s AS product, o AS p1 ;
f4 = FILTER bsbm_productPropertyNumeric3 BY (o < $y);
t4 = FOREACH f4 GENERATE s AS product, o AS p3 ;
BGP1 = JOIN t2 BY product, t1 BY product, t3 BY product, t4 BY product, t0 BY product ;
BGP1 = FOREACH BGP1 GENERATE t0::product AS product, t0::label AS label ;

-- BGP
f0 = FILTER bsbm_productFeature BY o == '$ProductFeature2' ;
t0 = FOREACH f0 GENERATE s AS product ;
t1 = FOREACH rdfs_label GENERATE s AS product, o AS testVar ;
BGP2 = JOIN t0 BY product, t1 BY product ;
BGP2 = FOREACH BGP2 GENERATE t0::product AS product, t1::testVar AS testVar ;

-- OPTIONAL
lj = JOIN BGP1 BY product LEFT OUTER, BGP2 BY product ;
OPTIONAL1 = FOREACH lj GENERATE BGP1::product AS product, BGP1::label AS label, BGP2::testVar AS testVar ;

-- FILTER
FILTER1 = FILTER OPTIONAL1 BY (testVar is null) ;

-- SM_Project
SM_Project = FOREACH FILTER1 GENERATE product, label ;

-- SM_Order
SM_Order = ORDER SM_Project BY label ;

-- SM_Slice
SM_Slice = LIMIT SM_Order 10 ;

-- store results into output
STORE SM_Slice INTO '$outputData' USING PigStorage('\t') ;
