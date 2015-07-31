%default reducerNum '1';

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
ub_memberOf = LOAD '$inputData/ub_memberOf' USING PigStorage(' ') AS (s,o) ;
ub_subOrganizationOf = LOAD '$inputData/ub_subOrganizationOf' USING PigStorage(' ') AS (s,o) ;
ub_undergraduateDegreeFrom = LOAD '$inputData/ub_undergraduateDegreeFrom' USING PigStorage(' ') AS (s,o) ;

-- BGP
-- ?Y rdf:type ub:University
f0 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>' ;
t0 = FOREACH f0 GENERATE s AS y ;

-- ?Z ub:subOrganizationOf ?Y
t1 = FOREACH ub_subOrganizationOf GENERATE s AS z, o AS y ;

-- ?Z rdf:type ub:Department
f2 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>' ;
t2 = FOREACH f2 GENERATE s AS z ;

-- ?X ub:memberOf ?Z
t3 = FOREACH ub_memberOf GENERATE s AS x, o AS z ;

-- ?X rdf:type ub:GraduateStudent
f4 = FILTER rdf_type BY o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>' ;
t4 = FOREACH f4 GENERATE s AS x ;

-- ?X ub:undergraduateDegreeFrom ?Y
t5 = FOREACH ub_undergraduateDegreeFrom GENERATE s AS x, o AS y ;


BGP1 = JOIN t3 BY x, t4 BY x, t5 BY x PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE t3::x AS x, t5::y AS y, t3::z AS z ;

BGP2 = JOIN BGP1 BY (y,z), t1 BY (y,z) PARALLEL $reducerNum ;
BGP2 = FOREACH BGP2 GENERATE BGP1::x AS x, BGP1::y AS y, BGP1::z AS z ;

BGP3 = JOIN BGP2 BY z, t2 BY z PARALLEL $reducerNum ;
BGP3 = FOREACH BGP3 GENERATE BGP2::x AS x, BGP2::y AS y, BGP2::z AS z ;

BGP4 = JOIN BGP3 BY y, t0 BY y PARALLEL $reducerNum ;
BGP4 = FOREACH BGP4 GENERATE BGP3::x AS x, BGP3::y AS y, BGP3::z AS z ;


-- store results into output
STORE BGP4 INTO '$outputData' USING PigStorage(' ') ;
