%default reducerNum '1';

-- BGP
l0 = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
f0 = FILTER l0 BY o == 'bench:Article.' ;
t0 = FOREACH f0 GENERATE s AS article ;
t1 = LOAD '$inputData/dc_creator' USING PigStorage(' ') AS (article,person) ;
t2 = LOAD '$inputData/foaf_name' USING PigStorage(' ') AS (person,name) ;
t3 = LOAD '$inputData/foaf_name' USING PigStorage(' ') AS (person2,name) ;
t4 = LOAD '$inputData/dc_creator' USING PigStorage(' ') AS (inproc,person2) ;
l5 = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
f5 = FILTER l5 BY o == 'bench:Inproceedings.' ;
t5 = FOREACH f5 GENERATE s AS inproc ;
BGP1 = JOIN t0 BY article, t1 BY article PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS article, $2 AS person ;
BGP1 = JOIN t2 BY person, BGP1 BY person PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $1 AS name, $2 AS article, $3 AS person ;
BGP1 = JOIN t3 BY name, BGP1 BY name PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS person2, $2 AS name, $3 AS article, $4 AS person ;
BGP1 = JOIN t4 BY person2, BGP1 BY person2 PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS inproc, $2 AS person2, $3 AS name, $4 AS article, $5 AS person ;
BGP1 = JOIN t5 BY inproc, BGP1 BY inproc PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $1 AS inproc, $2 AS person2, $3 AS name, $4 AS article, $5 AS person ;

-- SM_Project
SM_Project = FOREACH BGP1 GENERATE person, name ;

-- SM_Distinct
SM_Distinct = DISTINCT SM_Project PARALLEL $reducerNum ;

-- store results into output
STORE SM_Distinct INTO '$outputData' USING PigStorage(' ') ;
