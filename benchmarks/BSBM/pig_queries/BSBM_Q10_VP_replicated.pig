SET default_parallel $reducerNum ;
SET job.name 'BSBM Q10 replicated ON $inputData' ;

-- load input data
bsbm_country = LOAD '$inputData/bsbm_country' USING PigStorage('\t') AS (s,o) ;
bsbm_deliveryDays = LOAD '$inputData/bsbm_deliveryDays' USING PigStorage('\t') AS (s,o) ;
bsbm_price = LOAD '$inputData/bsbm_price' USING PigStorage('\t') AS (s,o) ;
bsbm_product = LOAD '$inputData/bsbm_product' USING PigStorage('\t') AS (s,o) ;
bsbm_validTo = LOAD '$inputData/bsbm_validTo' USING PigStorage('\t') AS (s,o:datetime) ;
bsbm_vendor = LOAD '$inputData/bsbm_vendor' USING PigStorage('\t') AS (s,o) ;
dc_publisher = LOAD '$inputData/dc_publisher' USING PigStorage('\t') AS (s,o) ;


-- BGP
f0 = FILTER bsbm_product BY o == '$ProductXYZ' ;
t0 = FOREACH f0 GENERATE s AS offer ;
t1 = FOREACH bsbm_vendor GENERATE s AS offer, o AS vendor ;
t2 = FOREACH dc_publisher GENERATE s AS offer, o AS vendor ;
f3 = FILTER bsbm_country BY o == '<http://downlode.org/rdf/iso-3166/countries#US>' ;
t3 = FOREACH f3 GENERATE s AS vendor ;
f4 = FILTER bsbm_deliveryDays BY o <= 3 ;
t4 = FOREACH f4 GENERATE s AS offer ;
t5 = FOREACH bsbm_price GENERATE s AS offer, o AS price ;
f6 = FILTER bsbm_validTo BY o > ToDate('$currentDate') ;
t6 = FOREACH f6 GENERATE s AS offer ;

BGP1 = JOIN t0 BY offer, t4 BY offer, t6 BY offer, t1 BY offer, t5 BY offer ;
BGP1 = FOREACH BGP1 GENERATE t0::offer AS offer, t1::vendor AS vendor, t5::price AS price ;
BGP1 = JOIN t2 BY (offer, vendor), BGP1 BY (offer, vendor) USING 'replicated' ;
BGP1 = FOREACH BGP1 GENERATE BGP1::offer AS offer, BGP1::vendor AS vendor, BGP1::price AS price ;
BGP1 = JOIN t3 BY vendor, BGP1 BY vendor USING 'replicated' ;
BGP1 = FOREACH BGP1 GENERATE BGP1::offer AS offer, BGP1::price AS price ;

-- SM_Distinct
SM_Distinct = DISTINCT BGP1 ;

-- SM_Order
SM_Order = ORDER SM_Distinct BY price ;

-- SM_Slice
SM_Slice = LIMIT SM_Order 10 ;

-- store results into output
STORE SM_Slice INTO '$outputData' USING PigStorage('\t') ;
