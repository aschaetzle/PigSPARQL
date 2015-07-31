%default reducerNum '1';

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

-- BGP
f0 = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
                  AND o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Chair>' ;
t0 = FOREACH f0 GENERATE s AS x ;
f1 = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>' ;
t1 = FOREACH f1 GENERATE s AS x, o AS y ;
f2 = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
                  AND o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>' ;
t2 = FOREACH f2 GENERATE s AS y ;
f3 = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>'
                  AND o == '<http://www.University0.edu>' ;
t3 = FOREACH f3 GENERATE s AS y ;

BGP1 = JOIN t0 BY x, t1 BY x PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS x, $2 AS y ;

BGP2 = JOIN BGP1 BY y, t2 BY y PARALLEL $reducerNum ;
BGP2 = FOREACH BGP2 GENERATE $0 AS x, $1 AS y ;

BGP3 = JOIN BGP2 BY y, t3 BY y PARALLEL $reducerNum ;
BGP3 = FOREACH BGP3 GENERATE $0 AS x, $1 AS y ;

-- store results into output
STORE BGP3 INTO '$outputData' USING PigStorage() ;
