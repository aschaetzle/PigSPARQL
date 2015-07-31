SET default_parallel $reducerNum ;
SET job.name 'LUBM Q9 ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
ub_advisor = LOAD '$inputData/ub_advisor' USING PigStorage('\t') AS (s,o) ;
ub_takesCourse = LOAD '$inputData/ub_takesCourse' USING PigStorage('\t') AS (s,o) ;
ub_teacherOf = LOAD '$inputData/ub_teacherOf' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER rdf_type BY o == 'ub:Course' ;
t0 = FOREACH f0 GENERATE s AS z ;

t1 = FOREACH ub_teacherOf GENERATE s AS y, o AS z ;

f2 = FILTER rdf_type BY o == 'ub:Faculty' ;
t2 = FOREACH f2 GENERATE s AS y ;

t3 = FOREACH ub_advisor GENERATE s AS x, o AS y ;

f4 = FILTER rdf_type BY o == 'ub:Student' ;
t4 = FOREACH f4 GENERATE s AS x ;

t5 = FOREACH ub_takesCourse GENERATE s AS x, o AS z ;


BGP1 = JOIN t2 BY y, t1 BY y, t3 BY y ;
BGP1 = FOREACH BGP1 GENERATE t3::x AS x, t2::y AS y, t1::z AS z ;

BGP2 = JOIN BGP1 BY (x,z), t5 BY (x,z) ;
BGP2 = FOREACH BGP2 GENERATE BGP1::x AS x, BGP1::y AS y, BGP1::z AS z ;

BGP3 = JOIN BGP2 BY z, t0 BY z ;
BGP3 = FOREACH BGP3 GENERATE BGP2::x AS x, BGP2::y AS y, BGP2::z AS z ;

BGP4 = JOIN BGP3 BY x, t4 BY x ;
BGP4 = FOREACH BGP4 GENERATE BGP3::x AS x, BGP3::y AS y, BGP3::z AS z ;


-- store results into output
STORE BGP4 INTO '$outputData' USING PigStorage('\t') ;
