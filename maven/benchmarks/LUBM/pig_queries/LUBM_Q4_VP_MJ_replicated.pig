SET default_parallel $reducerNum ;
SET job.name 'LUBM Q4 replicated ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
ub_emailAddress = LOAD '$inputData/ub_emailAddress' USING PigStorage('\t') AS (s,o) ;
ub_name = LOAD '$inputData/ub_name' USING PigStorage('\t') AS (s,o) ;
ub_telephone = LOAD '$inputData/ub_telephone' USING PigStorage('\t') AS (s,o) ;
ub_worksFor = LOAD '$inputData/ub_worksFor' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER rdf_type BY o == 'ub:Professor' ;
t0 = FOREACH f0 GENERATE s AS x ;
f1 = FILTER ub_worksFor BY o == '<http://www.Department0.University0.edu>' ;
t1 = FOREACH f1 GENERATE s AS x ;
t2 = FOREACH ub_name GENERATE s AS x, o AS y1 ;
t3 = FOREACH ub_emailAddress GENERATE s AS x, o AS y2 ;
t4 = FOREACH ub_telephone GENERATE s AS x, o AS y3 ;


BGP = JOIN t2 BY x, t3 BY x, t4 BY x, t0 BY x, t1 BY x USING 'replicated' ;
BGP = FOREACH BGP GENERATE t1::x AS x, t2::y1 AS y1, t3::y2 AS y2, t4::y3 AS y3 ;


-- store results into output
STORE BGP INTO '$outputData' USING PigStorage('\t') ;
