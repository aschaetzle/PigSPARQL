SET default_parallel $reducerNum ;
SET job.name 'LUBM Q13 ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
ub_hasAlumnus = LOAD '$inputData/ub_hasAlumnus' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER ub_hasAlumnus BY s == '<http://www.University0.edu>' ;
t0 = FOREACH f0 GENERATE o AS x ;

f1 = FILTER rdf_type BY o == 'ub:Person' ;
t1 = FOREACH f1 GENERATE s AS x ;


BGP = JOIN t0 BY x, t1 BY x ;
BGP = FOREACH BGP GENERATE t0::x AS x ;

-- store results into output
STORE BGP INTO '$outputData' USING PigStorage('\t') ;
