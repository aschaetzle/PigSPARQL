SET default_parallel $reducerNum ;
SET job.name 'LUBM Q1 replicated ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
ub_takesCourse = LOAD '$inputData/ub_takesCourse' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER ub_takesCourse BY o == '<http://www.Department0.University0.edu/GraduateCourse0>' ;
t0 = FOREACH f0 GENERATE s AS x ;

f1 = FILTER rdf_type BY o == 'ub:GraduateStudent' ;
t1 = FOREACH f1 GENERATE s AS x ;

BGP = JOIN t1 BY x, t0 BY x USING 'replicated' ;
BGP = FOREACH BGP GENERATE t0::x AS x ;

-- store results into output
STORE BGP INTO '$outputData' USING PigStorage('\t') ;
