SET default_parallel $reducerNum ;
SET job.name 'BSBM Q2 ON $inputData' ;

-- load input data
bsbm_producer = LOAD '$inputData/bsbm_producer' USING PigStorage('\t') AS (s,o) ;
bsbm_productFeature = LOAD '$inputData/bsbm_productFeature' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric1 = LOAD '$inputData/bsbm_productPropertyNumeric1' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric2 = LOAD '$inputData/bsbm_productPropertyNumeric2' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyNumeric4 = LOAD '$inputData/bsbm_productPropertyNumeric4' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyTextual1 = LOAD '$inputData/bsbm_productPropertyTextual1' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyTextual2 = LOAD '$inputData/bsbm_productPropertyTextual2' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyTextual3 = LOAD '$inputData/bsbm_productPropertyTextual3' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyTextual4 = LOAD '$inputData/bsbm_productPropertyTextual4' USING PigStorage('\t') AS (s,o) ;
bsbm_productPropertyTextual5 = LOAD '$inputData/bsbm_productPropertyTextual5' USING PigStorage('\t') AS (s,o) ;
dc_publisher = LOAD '$inputData/dc_publisher' USING PigStorage('\t') AS (s,o) ;
rdfs_comment = LOAD '$inputData/rdfs_comment' USING PigStorage('\t') AS (s,o) ;
rdfs_label = LOAD '$inputData/rdfs_label' USING PigStorage('\t') AS (s,o) ;


-- BGP
f0 = FILTER rdfs_label BY s == '$ProductXYZ' ;
t0 = FOREACH f0 GENERATE s AS product, o AS label ;
f1 = FILTER rdfs_comment BY s == '$ProductXYZ' ;
t1 = FOREACH f1 GENERATE s AS product, o AS comment ;
f2 = FILTER bsbm_producer BY s == '$ProductXYZ' ;
t2 = FOREACH f2 GENERATE s AS product, o AS p ;
t3 = FOREACH rdfs_label GENERATE s AS p, o AS producer ;
f4 = FILTER dc_publisher BY s == '$ProductXYZ' ;
t4 = FOREACH f4 GENERATE s AS product, o AS p ;
f5 = FILTER bsbm_productFeature BY s == '$ProductXYZ' ;
t5 = FOREACH f5 GENERATE s AS product, o AS f ;
t6 = FOREACH rdfs_label GENERATE s AS f, o AS productFeature ;
f7 = FILTER bsbm_productPropertyTextual1 BY s == '$ProductXYZ' ;
t7 = FOREACH f7 GENERATE s AS product, o AS propertyTextual1 ;
f8 = FILTER bsbm_productPropertyTextual2 BY s == '$ProductXYZ' ;
t8 = FOREACH f8 GENERATE s AS product, o AS propertyTextual2 ;
f9 = FILTER bsbm_productPropertyTextual3 BY s == '$ProductXYZ' ;
t9 = FOREACH f9 GENERATE s AS product, o AS propertyTextual3 ;
f10 = FILTER bsbm_productPropertyNumeric1 BY s == '$ProductXYZ' ;
t10 = FOREACH f10 GENERATE s AS product, o AS propertyNumeric1 ;
f11 = FILTER bsbm_productPropertyNumeric2 BY s == '$ProductXYZ' ;
t11 = FOREACH f11 GENERATE s AS product, o AS propertyNumeric2 ;

BGP1_0 = JOIN t0 BY product, t1 BY product, t2 BY product, t5 BY product, t7 BY product, t8 BY product, t9 BY product, t10 BY product, t11 BY product ;
BGP1_0 = FOREACH BGP1_0 GENERATE t0::product AS product, t0::label AS label, t1::comment AS comment, t2::p AS p, t5::f AS f, t7::propertyTextual1 AS propertyTextual1,
  t8::propertyTextual2 AS propertyTextual2, t9::propertyTextual3 AS propertyTextual3, t10::propertyNumeric1 AS propertyNumeric1,
  t11::propertyNumeric2 AS propertyNumeric2 ;
BGP1_1 = JOIN BGP1_0 BY p, t4 BY p, t3 BY p ;
BGP1_1 = FOREACH BGP1_1 GENERATE BGP1_0::product AS product, BGP1_0::label AS label, BGP1_0::comment AS comment, BGP1_0::p AS p, BGP1_0::f AS f, t3::producer AS producer, 
  BGP1_0::propertyTextual1 AS propertyTextual1, BGP1_0::propertyTextual2 AS propertyTextual2, BGP1_0::propertyTextual3 AS propertyTextual3, 
  BGP1_0::propertyNumeric1 AS propertyNumeric1, BGP1_0::propertyNumeric2 AS propertyNumeric2  ;
BGP1_2 = JOIN BGP1_1 BY f, t6 BY f ;
BGP1 = FOREACH BGP1_2 GENERATE BGP1_1::product AS product, BGP1_1::label AS label, BGP1_1::comment AS comment, BGP1_1::producer AS producer,
  t6::productFeature AS productFeature, BGP1_1::propertyTextual1 AS propertyTextual1, BGP1_1::propertyTextual2 AS propertyTextual2, 
  BGP1_1::propertyTextual3 AS propertyTextual3, BGP1_1::propertyNumeric1 AS propertyNumeric1, BGP1_1::propertyNumeric2 AS propertyNumeric2  ;

-- OPTIONAL
f0 = FILTER bsbm_productPropertyTextual4 BY s == '$ProductXYZ' ;
BGP2 = FOREACH f0 GENERATE s AS product, o AS propertyTextual4 ;
OPT1 = JOIN BGP1 BY product LEFT OUTER, BGP2 BY product ;
OPT1 = FOREACH OPT1 GENERATE BGP1::product AS product, BGP1::label AS label, BGP1::comment AS comment, BGP1::producer AS producer,
  BGP1::productFeature AS productFeature, BGP1::propertyTextual1 AS propertyTextual1, BGP1::propertyTextual2 AS propertyTextual2,
  BGP1::propertyTextual3 AS propertyTextual3, BGP1::propertyNumeric1 AS propertyNumeric1, BGP1::propertyNumeric2 AS propertyNumeric2,
  BGP2::propertyTextual4 AS propertyTextual4 ;

-- OPTIONAL
f0 = FILTER bsbm_productPropertyTextual5 BY s == '$ProductXYZ' ;
BGP3 = FOREACH f0 GENERATE s AS product, o AS propertyTextual5 ;
OPT2 = JOIN OPT1 BY product LEFT OUTER, BGP3 BY product ;
OPT2 = FOREACH OPT2 GENERATE OPT1::product AS product, OPT1::label AS label, OPT1::comment AS comment, OPT1::producer AS producer,
  OPT1::productFeature AS productFeature, OPT1::propertyTextual1 AS propertyTextual1, OPT1::propertyTextual2 AS propertyTextual2,
  OPT1::propertyTextual3 AS propertyTextual3, OPT1::propertyNumeric1 AS propertyNumeric1, OPT1::propertyNumeric2 AS propertyNumeric2,
  OPT1::propertyTextual4 AS propertyTextual4, BGP3::propertyTextual5 AS propertyTextual5 ;

-- OPTIONAL
f0 = FILTER bsbm_productPropertyNumeric4 BY s == '$ProductXYZ' ;
BGP4 = FOREACH f0 GENERATE s AS product, o AS propertyNumeric4 ;
OPT3 = JOIN OPT2 BY product LEFT OUTER, BGP4 BY product ;
OPT3 = FOREACH OPT3 GENERATE OPT2::product AS product, OPT2::label AS label, OPT2::comment AS comment, OPT2::producer AS producer,
  OPT2::productFeature AS productFeature, OPT2::propertyTextual1 AS propertyTextual1, OPT2::propertyTextual2 AS propertyTextual2,
  OPT2::propertyTextual3 AS propertyTextual3, OPT2::propertyNumeric1 AS propertyNumeric1, OPT2::propertyNumeric2 AS propertyNumeric2,
  OPT2::propertyTextual4 AS propertyTextual4, OPT2::propertyTextual5 AS propertyTextual5, BGP4::propertyNumeric4 AS propertyNumeric4 ;
  

-- store results into output
STORE OPT3 INTO '$outputData' USING PigStorage('\t') ;
