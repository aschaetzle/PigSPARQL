SET default_parallel $reducerNum ;
SET job.name 'LUBM Q12 replicated ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
ub_headOf = LOAD '$inputData/ub_headOf' USING PigStorage('\t') AS (s,o) ;
ub_subOrganizationOf = LOAD '$inputData/ub_subOrganizationOf' USING PigStorage('\t') AS (s,o) ;
ub_worksFor = LOAD '$inputData/ub_worksFor' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER rdf_type BY o == 'ub:Chair' ;
t0 = FOREACH f0 GENERATE s AS x ;

t1 = FOREACH ub_worksFor GENERATE s AS x, o AS y ;

f2 = FILTER rdf_type BY o == 'ub:Department' ;
t2 = FOREACH f2 GENERATE s AS y ;

f3 = FILTER ub_subOrganizationOf BY o == '<http://www.University0.edu>' ;
t3 = FOREACH f3 GENERATE s AS y ;


BGP1 = JOIN t1 BY x, t0 BY x USING 'replicated' ;
BGP1 = FOREACH BGP1 GENERATE t0::x AS x, t1::y AS y ;

BGP2 = JOIN t2 BY y, t3 BY y, BGP1 BY y USING 'replicated' ;
BGP2 = FOREACH BGP2 GENERATE BGP1::x AS x, BGP1::y AS y ;


-- store results into output
STORE BGP2 INTO '$outputData' USING PigStorage('\t') ;
