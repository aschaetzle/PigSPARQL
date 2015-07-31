%default reducerNum '1';

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
ub_takesCourse = LOAD '$inputData/ub_takesCourse' USING PigStorage(' ') AS (s,o) ;
ub_teacherOf = LOAD '$inputData/ub_teacherOf' USING PigStorage(' ') AS (s,o) ;

-- BGP
f0 = FILTER ub_teacherOf BY s == '<http://www.Department0.University0.edu/AssociateProfessor0>' ;
t0 = FOREACH f0 GENERATE o AS y ;

f1 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course>' ;
t1 = FOREACH f1 GENERATE s AS y ;

t2 = FOREACH ub_takesCourse GENERATE s AS x, o AS y ;

f3 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>' ;
t3 = FOREACH f3 GENERATE s AS x;


BGP1 = JOIN t0 BY y, t1 BY y, t2 BY y PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS y, $2 AS x ;

BGP2 = JOIN BGP1 BY x, t3 BY x PARALLEL $reducerNum ;
BGP2 = FOREACH BGP2 GENERATE $0 AS y, $1 AS x ;


-- store results into output
STORE BGP2 INTO '$outputData' USING PigStorage(' ') ;
