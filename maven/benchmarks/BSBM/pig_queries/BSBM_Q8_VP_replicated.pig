SET default_parallel $reducerNum ;
SET job.name 'BSBM Q8 replicated ON $inputData' ;

-- load input data
bsbm_rating1 = LOAD '$inputData/bsbm_rating1' USING PigStorage('\t') AS (s,o) ;
bsbm_rating2 = LOAD '$inputData/bsbm_rating2' USING PigStorage('\t') AS (s,o) ;
bsbm_rating3 = LOAD '$inputData/bsbm_rating3' USING PigStorage('\t') AS (s,o) ;
bsbm_rating4 = LOAD '$inputData/bsbm_rating4' USING PigStorage('\t') AS (s,o) ;
bsbm_reviewDate = LOAD '$inputData/bsbm_reviewDate' USING PigStorage('\t') AS (s,o) ;
bsbm_reviewFor = LOAD '$inputData/bsbm_reviewFor' USING PigStorage('\t') AS (s,o) ;
dc_title = LOAD '$inputData/dc_title' USING PigStorage('\t') AS (s,o) ;
foaf_name = LOAD '$inputData/foaf_name' USING PigStorage('\t') AS (s,o) ;
rev_reviewer = LOAD '$inputData/rev_reviewer' USING PigStorage('\t') AS (s,o) ;
rev_text = LOAD '$inputData/rev_text' USING PigStorage('\t') AS (s,o) ;


-- BGP
f0 = FILTER bsbm_reviewFor BY o == '$ProductXYZ' ;
t0 = FOREACH f0 GENERATE s AS review ;
t1 = FOREACH dc_title GENERATE s AS review, o AS title ;
f2 = FILTER rev_text BY o matches '.*en$' ;
t2 = FOREACH f2 GENERATE s AS review, o AS text ;
t3 = FOREACH bsbm_reviewDate GENERATE s AS review, o AS reviewDate ;
t4 = FOREACH rev_reviewer GENERATE s AS review, o AS reviewer ;
t5 = FOREACH foaf_name GENERATE s AS reviewer, o AS reviewerName ;
BGP1 = JOIN t0 BY review, t1 BY review, t3 BY review, t4 BY review, t2 BY review ;
BGP1 = FOREACH BGP1 GENERATE t0::review AS review, t1::title AS title, t2::text AS text, t3::reviewDate AS reviewDate, t4::reviewer AS reviewer ;
BGP1 = JOIN t5 BY reviewer, BGP1 BY reviewer USING 'replicated' ;
BGP1 = FOREACH BGP1 GENERATE BGP1::review AS review, BGP1::title AS title, BGP1::text AS text, BGP1::reviewDate AS reviewDate,
  BGP1::reviewer AS reviewer, t5::reviewerName AS reviewerName ;

-- BGP
BGP2 = FOREACH bsbm_rating1 GENERATE s AS review, o AS rating1 ;

-- OPTIONAL
lj = JOIN BGP1 BY review LEFT OUTER, BGP2 BY review ;
OPTIONAL1 = FOREACH lj GENERATE BGP1::review AS review, BGP1::title AS title, BGP1::text AS text, BGP1::reviewDate AS reviewDate, BGP1::reviewer AS reviewer,
  BGP1::reviewerName AS reviewerName, BGP2::rating1 AS rating1 ;

-- BGP
BGP3 = FOREACH bsbm_rating2 GENERATE s AS review, o AS rating2 ;

-- OPTIONAL
lj = JOIN OPTIONAL1 BY review LEFT OUTER, BGP3 BY review ;
OPTIONAL2 = FOREACH lj GENERATE OPTIONAL1::review AS review, OPTIONAL1::title AS title, OPTIONAL1::text AS text, OPTIONAL1::reviewDate AS reviewDate,
  OPTIONAL1::reviewer AS reviewer, OPTIONAL1::reviewerName AS reviewerName, OPTIONAL1::rating1 AS rating1, BGP3::rating2 AS rating2 ;

-- BGP
BGP4 = FOREACH bsbm_rating3 GENERATE s AS review, o AS rating3 ;

-- OPTIONAL
lj = JOIN OPTIONAL2 BY review LEFT OUTER, BGP4 BY review ;
OPTIONAL3 = FOREACH lj GENERATE OPTIONAL2::review AS review, OPTIONAL2::title AS title, OPTIONAL2::text AS text, OPTIONAL2::reviewDate AS reviewDate,
  OPTIONAL2::reviewer AS reviewer, OPTIONAL2::reviewerName AS reviewerName, OPTIONAL2::rating1 AS rating1, OPTIONAL2::rating2 AS rating2, BGP4::rating3 AS rating3 ;

-- BGP
BGP5 = FOREACH bsbm_rating4 GENERATE s AS review, o AS rating4 ;

-- OPTIONAL
lj = JOIN OPTIONAL3 BY review LEFT OUTER, BGP5 BY review ;
OPTIONAL4 = FOREACH lj GENERATE OPTIONAL3::title AS title, OPTIONAL3::text AS text, OPTIONAL3::reviewDate AS reviewDate, OPTIONAL3::reviewer AS reviewer,
  OPTIONAL3::reviewerName AS reviewerName, OPTIONAL3::rating1 AS rating1, OPTIONAL3::rating2 AS rating2, OPTIONAL3::rating3 AS rating3, BGP5::rating4 AS rating4 ;
  
-- SM_Order
SM_Order = ORDER OPTIONAL4 BY reviewDate DESC ;

-- SM_Slice
SM_Slice = LIMIT SM_Order 20 ;

-- store results into output
STORE SM_Slice INTO '$outputData' USING PigStorage('\t') ;
