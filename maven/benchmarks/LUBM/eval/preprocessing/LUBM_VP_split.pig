%default reducerNum '1';

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

-- Vertical Partitioning
SPLIT indata INTO
rdf_type IF p == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>',
ub_advisor IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor>',
ub_emailAddress IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress>',
ub_hasAlumnus IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#hasAlumnus>',
ub_headOf IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#headOf>',
ub_memberOf IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>',
ub_name IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>',
ub_publicationAuthor IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor>',
ub_subOrganizationOf IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>',
ub_takesCourse IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>',
ub_teacherOf IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>',
ub_telephone IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone>',
ub_undergraduateDegreeFrom IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>',
ub_worksFor IF p == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>' ;

-- Remove predicates in relations
rdf_type = FOREACH rdf_type GENERATE s,o ;
ub_advisor = FOREACH ub_advisor GENERATE s,o ;
ub_emailAddress = FOREACH ub_emailAddress GENERATE s,o ;
ub_hasAlumnus = FOREACH ub_hasAlumnus GENERATE s,o ;
ub_headOf = FOREACH ub_headOf GENERATE s,o ;
ub_memberOf = FOREACH ub_memberOf GENERATE s,o ;
ub_name = FOREACH ub_name GENERATE s,o ;
ub_publicationAuthor = FOREACH ub_publicationAuthor GENERATE s,o ;
ub_subOrganizationOf = FOREACH ub_subOrganizationOf GENERATE s,o ;
ub_takesCourse = FOREACH ub_takesCourse GENERATE s,o ;
ub_teacherOf = FOREACH ub_teacherOf GENERATE s,o ;
ub_telephone = FOREACH ub_telephone GENERATE s,o ;
ub_undergraduateDegreeFrom = FOREACH ub_undergraduateDegreeFrom GENERATE s,o ;
ub_worksFor = FOREACH ub_worksFor GENERATE s,o ;

-- store results into output
STORE rdf_type INTO '$outputData/rdf_type' USING PigStorage(' ') ;
STORE ub_advisor INTO '$outputData/ub_advisor' USING PigStorage(' ') ;
STORE ub_emailAddress INTO '$outputData/ub_emailAddress' USING PigStorage(' ') ;
STORE ub_hasAlumnus INTO '$outputData/ub_hasAlumnus' USING PigStorage(' ') ;
STORE ub_headOf INTO '$outputData/ub_headOf' USING PigStorage(' ') ;
STORE ub_memberOf INTO '$outputData/ub_memberOf' USING PigStorage(' ') ;
STORE ub_name INTO '$outputData/ub_name' USING PigStorage(' ') ;
STORE ub_publicationAuthor INTO '$outputData/ub_publicationAuthor' USING PigStorage(' ') ;
STORE ub_subOrganizationOf INTO '$outputData/ub_subOrganizationOf' USING PigStorage(' ') ;
STORE ub_takesCourse INTO '$outputData/ub_takesCourse' USING PigStorage(' ') ;
STORE ub_teacherOf INTO '$outputData/ub_teacherOf' USING PigStorage(' ') ;
STORE ub_telephone INTO '$outputData/ub_telephone' USING PigStorage(' ') ;
STORE ub_undergraduateDegreeFrom INTO '$outputData/ub_undergraduateDegreeFrom' USING PigStorage(' ') ;
STORE ub_worksFor INTO '$outputData/ub_worksFor' USING PigStorage(' ') ;
