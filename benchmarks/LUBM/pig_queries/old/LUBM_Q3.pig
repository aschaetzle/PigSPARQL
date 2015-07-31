%default reducerNum '1';

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

-- BGP
f0 = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor>'
                  AND o == '<http://www.Department0.University0.edu/AssistantProfessor0>' ;
t0 = FOREACH f0 GENERATE s AS x ;

f1 = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
                  AND o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication>' ;
t1 = FOREACH f1 GENERATE s AS x ;


BGP = JOIN t0 BY x, t1 BY x PARALLEL $reducerNum ;
BGP = FOREACH BGP GENERATE $0 AS x ;

-- store results into output
STORE BGP INTO '$outputData' USING PigStorage() ;
