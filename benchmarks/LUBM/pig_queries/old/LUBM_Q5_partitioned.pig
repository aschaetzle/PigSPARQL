%default reducerNum '1';

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
ub_memberOf = LOAD '$inputData/ub_memberOf' USING PigStorage(' ') AS (s,o) ;

-- BGP
f0 = FILTER ub_memberOf BY o == '<http://www.Department0.University0.edu>' ;
t0 = FOREACH f0 GENERATE s AS x ;

f1 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Person>' ;
t1 = FOREACH f1 GENERATE s AS x ;


BGP = JOIN t0 BY x, t1 BY x PARALLEL $reducerNum ;
BGP = FOREACH BGP GENERATE $0 AS x ;


-- store results into output
STORE BGP INTO '$outputData' USING PigStorage(' ') ;
