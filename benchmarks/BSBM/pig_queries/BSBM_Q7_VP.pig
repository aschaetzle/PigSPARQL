SET default_parallel $reducerNum ;
SET job.name 'BSBM Q7 ON $inputData' ;

-- load input data
bsbm_country = LOAD '$inputData/bsbm_country' USING PigStorage('\t') AS (s,o) ;
bsbm_price = LOAD '$inputData/bsbm_price' USING PigStorage('\t') AS (s,o) ;
bsbm_product = LOAD '$inputData/bsbm_product' USING PigStorage('\t') AS (s,o) ;
bsbm_rating1 = LOAD '$inputData/bsbm_rating1' USING PigStorage('\t') AS (s,o) ;
bsbm_rating2 = LOAD '$inputData/bsbm_rating2' USING PigStorage('\t') AS (s,o) ;
bsbm_reviewFor = LOAD '$inputData/bsbm_reviewFor' USING PigStorage('\t') AS (s,o) ;
bsbm_validTo = LOAD '$inputData/bsbm_validTo' USING PigStorage('\t') AS (s,o:datetime) ;
bsbm_vendor = LOAD '$inputData/bsbm_vendor' USING PigStorage('\t') AS (s,o) ;
dc_publisher = LOAD '$inputData/dc_publisher' USING PigStorage('\t') AS (s,o) ;
dc_title = LOAD '$inputData/dc_title' USING PigStorage('\t') AS (s,o) ;
foaf_name = LOAD '$inputData/foaf_name' USING PigStorage('\t') AS (s,o) ;
rev_reviewer = LOAD '$inputData/rev_reviewer' USING PigStorage('\t') AS (s,o) ;
rdfs_label = LOAD '$inputData/rdfs_label' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER rdfs_label BY s == '$ProductXYZ' ;
BGP1 = FOREACH f0 GENERATE s AS productXYZ, o AS productLabel ;

-- BGP + FILTER
f0 = FILTER bsbm_product BY o == '$ProductXYZ' ;
t0 = FOREACH f0 GENERATE s AS offer, o AS productXYZ ;
t1 = FOREACH bsbm_price GENERATE s AS offer, o AS price ;
t2 = FOREACH bsbm_vendor GENERATE s AS offer, o AS vendor ;
t3 = FOREACH rdfs_label GENERATE s AS vendor, o AS vendorTitle ;
f4 = FILTER bsbm_country BY o == '<http://downlode.org/rdf/iso-3166/countries#DE>' ;
t4 = FOREACH f4 GENERATE s AS vendor ;
t5 = FOREACH dc_publisher GENERATE s AS offer, o AS vendor ;
f6 = FILTER bsbm_validTo BY o > ToDate('$currentDate') ;
t6 = FOREACH f6 GENERATE s AS offer ;

j1 = JOIN t0 BY offer, t1 BY offer, t2 BY offer, t6 BY offer ;
j1 = FOREACH j1 GENERATE t0::offer AS offer, t0::productXYZ AS productXYZ, t1::price AS price, t2::vendor AS vendor ;
j2 = JOIN j1 BY (offer,vendor), t5 BY (offer,vendor) ;
j2 = FOREACH j2 GENERATE j1::offer AS offer, j1::productXYZ AS productXYZ, j1::price AS price, j1::vendor AS vendor ;
BGP2 = JOIN j2 BY vendor, t4 BY vendor, t3 BY vendor ;
BGP2 = FOREACH BGP2 GENERATE j2::offer AS offer, j2::productXYZ AS productXYZ, j2::price AS price, j2::vendor AS vendor, t3::vendorTitle AS vendorTitle ;


-- BGP
f7 = FILTER bsbm_reviewFor BY o == '$ProductXYZ' ;
t7 = FOREACH f7 GENERATE s AS review, o AS productXYZ ;
t8 = FOREACH rev_reviewer GENERATE s AS review, o AS reviewer ;
t9 = FOREACH foaf_name GENERATE s AS reviewer, o AS revName ;
t10 = FOREACH dc_title GENERATE s AS review, o AS revTitle ;

j1 = JOIN t7 BY review, t8 BY review, t10 BY review ;
j1 = FOREACH j1 GENERATE t7::review AS review, t7::productXYZ AS productXYZ, t8::reviewer AS reviewer, t10::revTitle AS revTitle ;
BGP3 = JOIN j1 BY reviewer, t9 BY reviewer ;
BGP3 = FOREACH BGP3 GENERATE j1::review AS review, j1::productXYZ AS productXYZ, j1::reviewer AS reviewer, t9::revName AS revName, j1::revTitle AS revTitle ;


-- BGP
BGP4 = FOREACH bsbm_rating1 GENERATE s AS review, o AS rating1 ;

-- BGP
BGP5 = FOREACH bsbm_rating2 GENERATE s AS review, o AS rating2 ;


-- OPTIONAL
LJ1 = JOIN BGP3 BY review LEFT OUTER, BGP4 BY review ;
LJ1 = FOREACH LJ1 GENERATE BGP3::review AS review, BGP3::productXYZ AS productXYZ, BGP3::reviewer AS reviewer, BGP3::revName AS revName,
  BGP3::revTitle AS revTitle, BGP4::rating1 AS rating1 ;
  
-- OPTIONAL
LJ2 = JOIN LJ1 BY review LEFT OUTER, BGP5 BY review ;
LJ2 = FOREACH LJ2 GENERATE LJ1::review AS review, LJ1::productXYZ AS productXYZ, LJ1::reviewer AS reviewer, LJ1::revName AS revName,
  LJ1::revTitle AS revTitle, LJ1::rating1 AS rating1, BGP5::rating2 AS rating2 ;

  
-- OPTIONAL
LJ3 = JOIN BGP1 BY productXYZ LEFT OUTER, BGP2 BY productXYZ ;
LJ3 = FOREACH LJ3 GENERATE BGP1::productXYZ AS productXYZ, BGP1::productLabel AS productLabel, BGP2::offer AS offer, BGP2::price AS price,
  BGP2::vendor AS vendor, BGP2::vendorTitle AS vendorTitle ;

-- OPTIONAL
LJ4 = JOIN LJ3 BY productXYZ LEFT OUTER, LJ2 BY productXYZ ;
LJ4 = FOREACH LJ4 GENERATE LJ3::productLabel AS productLabel, LJ3::offer AS offer, LJ3::price AS price, LJ3::vendor AS vendor, LJ3::vendorTitle AS vendorTitle,
  LJ2::review AS review, LJ2::reviewer AS reviewer, LJ2::revName AS revName, LJ2::revTitle AS revTitle, LJ2::rating1 AS rating1, LJ2::rating2 AS rating2 ;


-- store results into output
STORE LJ4 INTO '$outputData' USING PigStorage('\t') ;
