%default reducerNum '1';

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
ub_memberOf = LOAD '$inputData/ub_memberOf' USING PigStorage(' ') AS (s,o) ;
ub_subOrganizationOf = LOAD '$inputData/ub_subOrganizationOf' USING PigStorage(' ') AS (s,o) ;
ub_emailAddress = LOAD '$inputData/emailAddress' USING PigStorage(' ') AS (s,o) ;

-- BGP
f0 = FILTER ub_subOrganizationOf BY o == '<http://www.University0.edu>' ;
t0 = FOREACH f0 GENERATE s AS y ;

f1 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>' ;
t1 = FOREACH f1 GENERATE s AS y ;

t2 = FOREACH ub_memberOf GENERATE s AS x, o AS y ;

f3 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>' ;
t3 = FOREACH f3 GENERATE s AS x ;

t4 = FOREACH ub_emailAddress GENERATE s AS x, o AS z ;


BGP1 = JOIN t0 BY y, t1 BY y PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS y ;

BGP2 = JOIN BGP1 BY y, t2 BY y PARALLEL $reducerNum ;
BGP2 = FOREACH BGP2 GENERATE $0 AS y, $1 AS x ;

BGP3 = JOIN BGP2 BY x, t3 BY x PARALLEL $reducerNum ;
BGP3 = FOREACH BGP3 GENERATE $0 AS y, $1 AS x ;

BGP4 = JOIN BGP3 BY x, t4 BY x PARALLEL $reducerNum ;
BGP4 = FOREACH BGP4 GENERATE $0 AS y, $1 AS x, $3 AS z ;

-- store results into output
STORE BGP4 INTO '$outputData' USING PigStorage() ;
