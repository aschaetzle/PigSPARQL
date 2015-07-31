%default reducerNum '1';

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

-- BGP
f0 = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
                  AND o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>' ;
BGP = FOREACH f0 GENERATE s AS x ;


-- store results into output
STORE BGP INTO '$outputData' USING PigStorage() ;
