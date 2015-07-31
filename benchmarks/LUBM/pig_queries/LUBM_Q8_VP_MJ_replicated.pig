SET default_parallel $reducerNum ;
SET job.name 'LUBM Q8 replicated ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;
ub_memberOf = LOAD '$inputData/ub_memberOf' USING PigStorage('\t') AS (s,o) ;
ub_subOrganizationOf = LOAD '$inputData/ub_subOrganizationOf' USING PigStorage('\t') AS (s,o) ;
ub_emailAddress = LOAD '$inputData/ub_emailAddress' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER ub_subOrganizationOf BY o == '<http://www.University0.edu>' ;
t0 = FOREACH f0 GENERATE s AS y ;

f1 = FILTER rdf_type BY o == 'ub:Department' ;
t1 = FOREACH f1 GENERATE s AS y ;

t2 = FOREACH ub_memberOf GENERATE s AS x, o AS y ;

f3 = FILTER rdf_type BY o == 'ub:Student' ;
t3 = FOREACH f3 GENERATE s AS x ;

t4 = FOREACH ub_emailAddress GENERATE s AS x, o AS z ;


BGP1 = JOIN t0 BY y, t1 BY y, t2 By y ;
BGP1 = FOREACH BGP1 GENERATE t2::x AS x, t0::y AS y ;

BGP2 = JOIN t4 BY x, t3 BY x, BGP1 BY x USING 'replicated' ;
BGP2 = FOREACH BGP2 GENERATE BGP1::x AS x, BGP1::y AS y, t4::z AS z ;


-- store results into output
STORE BGP2 INTO '$outputData' USING PigStorage('\t') ;
