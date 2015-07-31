SET default_parallel $reducerNum ;
SET job.name 'LUBM Q7 replicated ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
ub_takesCourse = LOAD '$inputData/ub_takesCourse' USING PigStorage('\t') AS (s,o) ;
ub_teacherOf = LOAD '$inputData/ub_teacherOf' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER ub_teacherOf BY s == '<http://www.Department0.University0.edu/AssociateProfessor0>' ;
t0 = FOREACH f0 GENERATE o AS y ;

f1 = FILTER rdf_type BY o == 'ub:Course' ;
t1 = FOREACH f1 GENERATE s AS y ;

t2 = FOREACH ub_takesCourse GENERATE s AS x, o AS y ;

f3 = FILTER rdf_type BY o == 'ub:Student' ;
t3 = FOREACH f3 GENERATE s AS x;


BGP1 = JOIN t0 BY y, t1 BY y, t2 BY y ;
BGP1 = FOREACH BGP1 GENERATE t2::x AS x, t0::y AS y ;

BGP2 = JOIN t3 BY x, BGP1 BY x USING 'replicated' ;
BGP2 = FOREACH BGP2 GENERATE BGP1::x AS x, BGP1::y AS y ;


-- store results into output
STORE BGP2 INTO '$outputData' USING PigStorage('\t') ;
