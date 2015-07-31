%default reducerNum '1';

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

-- rdf:type
rdf_type = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' ;
rdf_type = FOREACH rdf_type GENERATE s,o ;
STORE rdf_type INTO '$outputData/rdf_type' USING PigStorage(' ') ;

-- ub:advisor
ub_advisor = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor>' ;
ub_advisor = FOREACH ub_advisor GENERATE s,o ;
STORE ub_advisor INTO '$outputData/ub_advisor' USING PigStorage(' ') ;

-- ub:emailAddress
ub_emailAddress = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress>' ;
ub_emailAddress = FOREACH ub_emailAddress GENERATE s,o ;
STORE ub_emailAddress INTO '$outputData/ub_emailAddress' USING PigStorage(' ') ;

-- ub:hasAlumnus
ub_hasAlumnus = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#hasAlumnus>' ;
ub_hasAlumnus = FOREACH ub_hasAlumnus GENERATE s,o ;
STORE ub_hasAlumnus INTO '$outputData/ub_hasAlumnus' USING PigStorage(' ') ;

-- ub:headOf
ub_headOf = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>' ;
ub_headOf = FOREACH ub_headOf GENERATE s,o ;
STORE ub_headOf INTO '$outputData/ub_headOf' USING PigStorage(' ') ;

-- ub:memberOf
ub_memberOf = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>' ;
ub_memberOf = FOREACH ub_memberOf GENERATE s,o ;
STORE ub_memberOf INTO '$outputData/ub_memberOf' USING PigStorage(' ') ;

-- ub:name
ub_name = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>' ;
ub_name = FOREACH ub_name GENERATE s,o ;
STORE ub_name INTO '$outputData/ub_name' USING PigStorage(' ') ;

-- ub:publicationAuthor
ub_publicationAuthor = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor>' ;
ub_publicationAuthor = FOREACH ub_publicationAuthor GENERATE s,o ;
STORE ub_publicationAuthor INTO '$outputData/ub_publicationAuthor' USING PigStorage(' ') ;

-- ub:subOrganizationOf
ub_subOrganizationOf = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>' ;
ub_subOrganizationOf = FOREACH ub_subOrganizationOf GENERATE s,o ;
STORE ub_subOrganizationOf INTO '$outputData/ub_subOrganizationOf' USING PigStorage(' ') ;

-- ub:takesCourse
ub_takesCourse = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>' ;
ub_takesCourse = FOREACH ub_takesCourse GENERATE s,o ;
STORE ub_takesCourse INTO '$outputData/ub_takesCourse' USING PigStorage(' ') ;

-- ub:teacherOf
ub_teacherOf = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>' ;
ub_teacherOf = FOREACH ub_teacherOf GENERATE s,o ;
STORE ub_teacherOf INTO '$outputData/ub_teacherOf' USING PigStorage(' ') ;

-- ub:telephone
ub_telephone = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone>' ;
ub_telephone = FOREACH ub_telephone GENERATE s,o ;
STORE ub_telephone INTO '$outputData/ub_telephone' USING PigStorage(' ') ;

-- ub:undergraduateDegreeFrom
ub_undergraduateDegreeFrom = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>' ;
ub_undergraduateDegreeFrom = FOREACH ub_undergraduateDegreeFrom GENERATE s,o ;
STORE ub_undergraduateDegreeFrom INTO '$outputData/ub_undergraduateDegreeFrom' USING PigStorage(' ') ;

-- ub:worksFor
ub_worksFor = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>' ;
ub_worksFor = FOREACH ub_worksFor GENERATE s,o ;
STORE ub_worksFor INTO '$outputData/ub_worksFor' USING PigStorage(' ') ;
