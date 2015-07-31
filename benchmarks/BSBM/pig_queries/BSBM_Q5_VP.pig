SET default_parallel $reducerNum ;
SET job.name 'BSBM Q5 ON $inputData' ;

-- load input data
bsbm_productFeature = LOAD '$inputData/bsbm_productFeature' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric1 = LOAD '$inputData/bsbm_productPropertyNumeric1' USING PigStorage('\t') AS (s,o:int) ;
bsbm_productPropertyNumeric2 = LOAD '$inputData/bsbm_productPropertyNumeric2' USING PigStorage('\t') AS (s,o:int) ;
rdfs_label = LOAD '$inputData/rdfs_label' USING PigStorage('\t') AS (s,o) ;


-- BGP
f0 = FILTER rdfs_label BY (s != '$ProductXYZ') ; 
t0 = FOREACH f0 GENERATE s AS product, o AS productLabel ;
f1 = FILTER bsbm_productFeature BY s == '$ProductXYZ' ;
t1 = FOREACH f1 GENERATE s AS productXYZ, o AS prodFeature;
t2 = FOREACH bsbm_productFeature GENERATE s AS product, o AS prodFeature;
f3 = FILTER bsbm_productPropertyNumeric1 BY s == '$ProductXYZ' ;
t3 = FOREACH f3 GENERATE s AS productXYZ, o AS origProperty1;
t4 = FOREACH bsbm_productPropertyNumeric1 GENERATE s AS product, o AS simProperty1;
f5 = FILTER bsbm_productPropertyNumeric2 BY s == '$ProductXYZ' ;
t5 = FOREACH f5 GENERATE s AS productXYZ, o AS origProperty2;
t6 = FOREACH bsbm_productPropertyNumeric2 GENERATE s AS product, o AS simProperty2;
BGP1 = JOIN t3 BY productXYZ, t5 BY productXYZ, t1 BY productXYZ ;
BGP1 = FOREACH BGP1 GENERATE t1::prodFeature AS prodFeature, t3::origProperty1 AS origProperty1, t5::origProperty2 AS origProperty2 ;
BGP2 = JOIN t4 BY product, t6 BY product, t2 BY product, t0 BY product ;
BGP2 = FOREACH BGP2 GENERATE t0::product AS product, t0::productLabel AS productLabel, t2::prodFeature AS prodFeature,
  t4::simProperty1 AS simProperty1, t6::simProperty2 AS simProperty2 ;

-- JOIN
j1 = JOIN BGP1 BY prodFeature, BGP2 BY prodFeature ;
j1 = FOREACH j1 GENERATE BGP2::product AS product, BGP2::productLabel AS productLabel,
  BGP1::origProperty1 AS origProperty1, BGP1::origProperty2 AS origProperty2, BGP2::simProperty1 AS simProperty1, BGP2::simProperty2 AS simProperty2 ;
  
-- FILTER
FILTER1 = FILTER j1 BY (simProperty1 < (origProperty1 + 120)) AND (simProperty1 > (origProperty1 - 120))
  AND (simProperty2 < (origProperty2 + 170)) AND (simProperty2 > (origProperty2 - 170)) ;

-- SM_Project
SM_Project = FOREACH FILTER1 GENERATE product, productLabel ;

-- SM_Distinct
SM_Distinct = DISTINCT SM_Project ;

-- SM_Order
SM_Order = ORDER SM_Distinct BY productLabel ;

-- SM_Slice
SM_Slice = LIMIT SM_Order 5 ;

-- store results into output
STORE SM_Slice INTO '$outputData' USING PigStorage('\t') ;
