%default reducerNum '1' ;
SET default_parallel $reducerNum ;

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

-- rdf:type
rdf_type = FILTER indata BY p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' ;
rdf_type = FOREACH rdf_type GENERATE s,o ;
rdf_type_s = ORDER rdf_type BY s ASC;
STORE rdf_type_s INTO '$outputData/rdf_type/sort_s' USING PigStorage(' ') ;
rdf_type_o = ORDER rdf_type BY o ASC;
STORE rdf_type_o INTO '$outputData/rdf_type/sort_o' USING PigStorage(' ') ;

-- ub:advisor
ub_advisor = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor>' ;
ub_advisor = FOREACH ub_advisor GENERATE s,o ;
ub_advisor_s = ORDER ub_advisor BY s ASC;
STORE ub_advisor_s INTO '$outputData/ub_advisor/sort_s' USING PigStorage(' ') ;
ub_advisor_o = ORDER ub_advisor BY o ASC;
STORE ub_advisor_o INTO '$outputData/ub_advisor/sort_o' USING PigStorage(' ') ;

-- ub:emailAddress
ub_emailAddress = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress>' ;
ub_emailAddress = FOREACH ub_emailAddress GENERATE s,o ;
ub_emailAddress_s = ORDER ub_emailAddress BY s ASC;
STORE ub_emailAddress_s INTO '$outputData/ub_emailAddress/sort_s' USING PigStorage(' ') ;
ub_emailAddress_o = ORDER ub_emailAddress BY o ASC;
STORE ub_emailAddress_o INTO '$outputData/ub_emailAddress/sort_o' USING PigStorage(' ') ;

-- ub:hasAlumnus
ub_hasAlumnus = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#hasAlumnus>' ;
ub_hasAlumnus = FOREACH ub_hasAlumnus GENERATE s,o ;
ub_hasAlumnus_s = ORDER ub_hasAlumnus BY s ASC;
STORE ub_hasAlumnus_s INTO '$outputData/ub_hasAlumnus/sort_s' USING PigStorage(' ') ;
ub_hasAlumnus_o = ORDER ub_hasAlumnus BY o ASC;
STORE ub_hasAlumnus_o INTO '$outputData/ub_hasAlumnus/sort_o' USING PigStorage(' ') ;

-- ub:headOf
ub_headOf = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>' ;
ub_headOf = FOREACH ub_headOf GENERATE s,o ;
ub_headOf_s = ORDER ub_headOf BY s ASC;
STORE ub_headOf_s INTO '$outputData/ub_headOf/sort_s' USING PigStorage(' ') ;
ub_headOf_o = ORDER ub_headOf BY o ASC;
STORE ub_headOf_o INTO '$outputData/ub_headOf/sort_o' USING PigStorage(' ') ;

-- ub:memberOf
ub_memberOf = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>' ;
ub_memberOf = FOREACH ub_memberOf GENERATE s,o ;
ub_memberOf_s = ORDER ub_memberOf BY s ASC;
STORE ub_memberOf_s INTO '$outputData/ub_memberOf/sort_s' USING PigStorage(' ') ;
ub_memberOf_o = ORDER ub_memberOf BY o ASC;
STORE ub_memberOf_o INTO '$outputData/ub_memberOf/sort_o' USING PigStorage(' ') ;

-- ub:name
ub_name = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>' ;
ub_name = FOREACH ub_name GENERATE s,o ;
ub_name_s = ORDER ub_name BY s ASC;
STORE ub_name_s INTO '$outputData/ub_name/sort_s' USING PigStorage(' ') ;
ub_name_o = ORDER ub_name BY o ASC;
STORE ub_name_o INTO '$outputData/ub_name/sort_o' USING PigStorage(' ') ;

-- ub:publicationAuthor
ub_publicationAuthor = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor>' ;
ub_publicationAuthor = FOREACH ub_publicationAuthor GENERATE s,o ;
ub_publicationAuthor_s = ORDER ub_publicationAuthor BY s ASC;
STORE ub_publicationAuthor_s INTO '$outputData/ub_publicationAuthor/sort_s' USING PigStorage(' ') ;
ub_publicationAuthor_o = ORDER ub_publicationAuthor BY o ASC;
STORE ub_publicationAuthor_o INTO '$outputData/ub_publicationAuthor/sort_o' USING PigStorage(' ') ;

-- ub:subOrganizationOf
ub_subOrganizationOf = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>' ;
ub_subOrganizationOf = FOREACH ub_subOrganizationOf GENERATE s,o ;
ub_subOrganizationOf_s = ORDER ub_subOrganizationOf BY s ASC;
STORE ub_subOrganizationOf_s INTO '$outputData/ub_subOrganizationOf/sort_s' USING PigStorage(' ') ;
ub_subOrganizationOf_o = ORDER ub_subOrganizationOf BY o ASC;
STORE ub_subOrganizationOf_o INTO '$outputData/ub_subOrganizationOf/sort_o' USING PigStorage(' ') ;

-- ub:takesCourse
ub_takesCourse = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>' ;
ub_takesCourse = FOREACH ub_takesCourse GENERATE s,o ;
ub_takesCourse_s = ORDER ub_takesCourse BY s ASC;
STORE ub_takesCourse_s INTO '$outputData/ub_takesCourse/sort_s' USING PigStorage(' ') ;
ub_takesCourse_o = ORDER ub_takesCourse BY o ASC;
STORE ub_takesCourse_o INTO '$outputData/ub_takesCourse/sort_o' USING PigStorage(' ') ;

-- ub:teacherOf
ub_teacherOf = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>' ;
ub_teacherOf = FOREACH ub_teacherOf GENERATE s,o ;
ub_teacherOf_s = ORDER ub_teacherOf BY s ASC;
STORE ub_teacherOf_s INTO '$outputData/ub_teacherOf/sort_s' USING PigStorage(' ') ;
ub_teacherOf_o = ORDER ub_teacherOf BY o ASC;
STORE ub_teacherOf_o INTO '$outputData/ub_teacherOf/sort_o' USING PigStorage(' ') ;

-- ub:telephone
ub_telephone = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone>' ;
ub_telephone = FOREACH ub_telephone GENERATE s,o ;
ub_telephone_s = ORDER ub_telephone BY s ASC;
STORE ub_telephone_s INTO '$outputData/ub_telephone/sort_s' USING PigStorage(' ') ;
ub_telephone_o = ORDER ub_telephone BY o ASC;
STORE ub_telephone_o INTO '$outputData/ub_telephone/sort_o' USING PigStorage(' ') ;

-- ub:undergraduateDegreeFrom
ub_undergraduateDegreeFrom = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>' ;
ub_undergraduateDegreeFrom = FOREACH ub_undergraduateDegreeFrom GENERATE s,o ;
ub_undergraduateDegreeFrom_s = ORDER ub_undergraduateDegreeFrom BY s ASC;
STORE ub_undergraduateDegreeFrom_s INTO '$outputData/ub_undergraduateDegreeFrom/sort_s' USING PigStorage(' ') ;
ub_undergraduateDegreeFrom_o = ORDER ub_undergraduateDegreeFrom BY o ASC;
STORE ub_undergraduateDegreeFrom_o INTO '$outputData/ub_undergraduateDegreeFrom/sort_o' USING PigStorage(' ') ;

-- ub:worksFor
ub_worksFor = FILTER indata BY p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>' ;
ub_worksFor = FOREACH ub_worksFor GENERATE s,o ;
ub_worksFor_s = ORDER ub_worksFor BY s ASC;
STORE ub_worksFor_s INTO '$outputData/ub_worksFor/sort_s' USING PigStorage(' ') ;
ub_worksFor_o = ORDER ub_worksFor BY o ASC;
STORE ub_worksFor_o INTO '$outputData/ub_worksFor/sort_o' USING PigStorage(' ') ;
