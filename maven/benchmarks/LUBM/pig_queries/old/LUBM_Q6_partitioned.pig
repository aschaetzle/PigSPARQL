%default reducerNum '1';

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;

-- BGP
f0 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>' ;
BGP = FOREACH f0 GENERATE s AS x ;


-- store results into output
STORE BGP INTO '$outputData' USING PigStorage(' ') ;
