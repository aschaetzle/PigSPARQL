%default reducerNum '1';

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
ub_emailAddress = LOAD '$inputData/ub_emailAddress' USING PigStorage(' ') AS (s,o) ;
ub_name = LOAD '$inputData/ub_name' USING PigStorage(' ') AS (s,o) ;
ub_telephone = LOAD '$inputData/ub_telephone' USING PigStorage(' ') AS (s,o) ;
ub_worksFor = LOAD '$inputData/ub_worksFor' USING PigStorage(' ') AS (s,o) ;

-- BGP
f0 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Professor>' ;
t0 = FOREACH f0 GENERATE s AS x ;
f1 = FILTER ub_worksFor BY o == '<http://www.Department0.University0.edu>' ;
t1 = FOREACH f1 GENERATE s AS x ;
t2 = FOREACH ub_name GENERATE s AS x, o AS y1 ;
t3 = FOREACH ub_emailAddress GENERATE s AS x, o AS y2 ;
t4 = FOREACH ub_telephone GENERATE s AS x, o AS y3 ;


BGP = JOIN t0 BY x, t1 BY x, t2 BY x, t3 BY x, t4 BY x PARALLEL $reducerNum ;
BGP = FOREACH BGP GENERATE $0 AS x, $3 AS y1, $5 AS y2, $7 AS y3 ;


-- store results into output
STORE BGP INTO '$outputData' USING PigStorage() ;
