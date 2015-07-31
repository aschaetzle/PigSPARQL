%default reducerNum '1';

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

-- BGP
f0 = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
                  AND o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Professor>' ;
t0 = FOREACH f0 GENERATE s AS x ;
f1 = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>'
                  AND o == '<http://www.Department0.University0.edu>' ;
t1 = FOREACH f1 GENERATE s AS x ;
f2 = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>' ;
t2 = FOREACH f2 GENERATE s AS x, o AS y1 ;
f3 = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress>' ;
t3 = FOREACH f3 GENERATE s AS x, o AS y2 ;
f4 = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone>' ;
t4 = FOREACH f4 GENERATE s AS x, o AS y3 ;


BGP = JOIN t0 BY x, t1 BY x, t2 BY x, t3 BY x, t4 BY x PARALLEL $reducerNum ;
BGP = FOREACH BGP GENERATE $0 AS x, $3 AS y1, $5 AS y2, $7 AS y3 ;


-- store results into output
STORE BGP INTO '$outputData' USING PigStorage() ;
