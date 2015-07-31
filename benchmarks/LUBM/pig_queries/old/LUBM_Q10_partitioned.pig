%default reducerNum '1';

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
ub_takesCourse = LOAD '$inputData/ub_takesCourse' USING PigStorage(' ') AS (s,o) ;

-- BGP
f0 = FILTER ub_takesCourse BY o == '<http://www.Department0.University0.edu/GraduateCourse0>' ;
t0 = FOREACH f0 GENERATE s AS x ;

f1 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>' ;
t1 = FOREACH f1 GENERATE s AS x ;


BGP = JOIN t0 BY x, t1 BY x PARALLEL $reducerNum ;
BGP = FOREACH BGP GENERATE $0 AS x ;

-- store results into output
STORE BGP INTO '$outputData' USING PigStorage(' ') ;
