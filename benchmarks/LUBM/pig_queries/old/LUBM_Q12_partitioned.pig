%default reducerNum '1';

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
ub_headOf = LOAD '$inputData/ub_headOf' USING PigStorage(' ') AS (s,o) ;
ub_subOrganizationOf = LOAD '$inputData/ub_subOrganizationOf' USING PigStorage(' ') AS (s,o) ;
ub_worksFor = LOAD '$inputData/ub_worksFor' USING PigStorage(' ') AS (s,o) ;

-- BGP
f0 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Chair>' ;
t0 = FOREACH f0 GENERATE s AS x ;

t1 = FOREACH ub_worksFor GENERATE s AS x, o AS y ;

f2 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>' ;
t2 = FOREACH f2 GENERATE s AS y ;

f3 = FILTER ub_subOrganizationOf BY o == '<http://www.University0.edu>' ;
t3 = FOREACH f3 GENERATE s AS y ;


BGP1 = JOIN t0 BY x, t1 BY x PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS x, $2 AS y ;

BGP2 = JOIN BGP1 BY y, t2 BY y PARALLEL $reducerNum ;
BGP2 = FOREACH BGP2 GENERATE $0 AS x, $1 AS y ;

BGP3 = JOIN BGP2 BY y, t3 BY y PARALLEL $reducerNum ;
BGP3 = FOREACH BGP3 GENERATE $0 AS x, $1 AS y ;

-- store results into output
STORE BGP3 INTO '$outputData' USING PigStorage() ;
