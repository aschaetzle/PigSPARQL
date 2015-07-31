%default reducerNum '1';

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

-- BGP
f0 = FILTER indata BY s == '<http://www.Department0.University0.edu/AssociateProfessor0>'
                  AND p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>' ;
t0 = FOREACH f0 GENERATE o AS y ;

f1 = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
                  AND o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course>' ;
t1 = FOREACH f1 GENERATE s AS y ;

f2 = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>' ;
t2 = FOREACH f2 GENERATE s AS x, o AS y ;

f3 = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
                  AND o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>' ;
t3 = FOREACH f3 GENERATE s AS x;


BGP1 = JOIN t0 BY y, t1 BY y PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS y ;

BGP2 = JOIN BGP1 BY y, t2 BY y PARALLEL $reducerNum ;
BGP2 = FOREACH BGP2 GENERATE $0 AS y, $1 AS x ;

BGP3 = JOIN BGP2 BY x, t3 BY x PARALLEL $reducerNum ;
BGP3 = FOREACH BGP3 GENERATE $0 AS y, $1 AS x ;


-- store results into output
STORE BGP3 INTO '$outputData' USING PigStorage() ;
