SET default_parallel $reducerNum ;
SET job.name 'BSBM Q4 replicated ON $inputData' ;

-- load input data
bsbm_productFeature = LOAD '$inputData/bsbm_productFeature' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric1 = LOAD '$inputData/bsbm_productPropertyNumeric1' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric2 = LOAD '$inputData/bsbm_productPropertyNumeric2' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyTextual1 = LOAD '$inputData/bsbm_productPropertyTextual1' USING PigStorage('\t') AS (s,o) ;
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
rdfs_label = LOAD '$inputData/rdfs_label' USING PigStorage('\t') AS (s,o) ;


-- BGP
t0 = FOREACH rdfs_label GENERATE s AS product, o AS label ;
f1 = FILTER rdf_type BY o == '$ProductType' ;
t1 = FOREACH f1 GENERATE s AS product ;
f2 = FILTER bsbm_productFeature BY o == '$ProductFeature1' ;
t2 = FOREACH f2 GENERATE s AS product ;
f3 = FILTER bsbm_productFeature BY o == '$ProductFeature2' ;
t3 = FOREACH f3 GENERATE s AS product ;
t4 = FOREACH bsbm_productPropertyTextual1 GENERATE s AS product, o AS propertyTextual ;
f5 = FILTER bsbm_productPropertyNumeric1 BY (o > $x) ;
t5 = FOREACH f5 GENERATE s AS product ;
BGP1 = JOIN t0 BY product, t5 BY product, t4 BY product, t1 BY product, t3 BY product, t2 BY product USING 'replicated' ;
BGP1 = FOREACH BGP1 GENERATE t0::product AS product, t0::label AS label, t4::propertyTextual AS propertyTextual ;

-- BGP
t0 = FOREACH rdfs_label GENERATE s AS product, o AS label ;
f1 = FILTER rdf_type BY o == '$ProductType' ;
t1 = FOREACH f1 GENERATE s AS product ;
f2 = FILTER bsbm_productFeature BY o == '$ProductFeature1' ;
t2 = FOREACH f2 GENERATE s AS product ;
f3 = FILTER bsbm_productFeature BY o == '$ProductFeature3' ;
t3 = FOREACH f3 GENERATE s AS product ;
t4 = FOREACH bsbm_productPropertyTextual1 GENERATE s AS product, o AS propertyTextual ;
f5 = FILTER bsbm_productPropertyNumeric2 BY (o > $y) ;
t5 = FOREACH f5 GENERATE s AS product ;
BGP2 = JOIN t0 BY product, t5 BY product, t4 BY product, t1 BY product, t3 BY product, t2 BY product USING 'replicated' ;
BGP2 = FOREACH BGP2 GENERATE t0::product AS product, t0::label AS label, t4::propertyTextual AS propertyTextual ;

-- UNION
UNION1 = UNION ONSCHEMA BGP1, BGP2 ;

-- SM_Distinct
SM_Distinct = DISTINCT UNION1 ;

-- SM_Order
SM_Order = ORDER SM_Distinct BY label ;

-- SM_Slice
SM_Slice = LIMIT SM_Order 15 ;

-- store results into output
STORE SM_Slice INTO '$outputData' USING PigStorage('\t') ;
