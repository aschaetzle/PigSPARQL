%default reducerNum '1';

-- BGP
type = LOAD '$inputData/rdf_type' USING PigStorage(' ') AS (s,o) ;
f0 = FILTER type BY o == 'bench:Inproceedings.' ;
t0 = FOREACH f0 GENERATE s AS inproc ;
t1 = LOAD '$inputData/dc_creator' USING PigStorage(' ') AS (inproc, author) ;
t2 = LOAD '$inputData/bench_booktitle' USING PigStorage(' ') AS (inproc, booktitle) ;
t3 = LOAD '$inputData/dc_title' USING PigStorage(' ') AS (inproc, title) ;
t4 = LOAD '$inputData/dcterms_partOf' USING PigStorage(' ') AS (inproc, proc) ;
t5 = LOAD '$inputData/rdfs_seeAlso' USING PigStorage(' ') AS (inproc, ee) ;
t6 = LOAD '$inputData/swrc_pages' USING PigStorage(' ') AS (inproc, page) ;
t7 = LOAD '$inputData/foaf_homepage' USING PigStorage(' ') AS (inproc, url) ;
t8 = LOAD '$inputData/dcterms_issued' USING PigStorage(' ') AS (inproc, yr) ;
BGP1 = JOIN t0 BY inproc, t1 BY inproc, t2 BY inproc, t3 BY inproc, t4 BY inproc, t5 BY inproc, t6 BY inproc, t7 BY inproc, t8 BY inproc PARALLEL $reducerNum ;
BGP1 = FOREACH BGP1 GENERATE $0 AS inproc, $2 AS author, $4 AS booktitle, $6 AS title, $8 AS proc, $10 AS ee, $12 AS page, $14 AS url, $16 AS yr ;

-- BGP
BGP2 = LOAD '$inputData/bench_abstract' USING PigStorage(' ') AS (inproc, abstract) ;

-- OPTIONAL
j1 = JOIN BGP1 BY inproc LEFT OUTER, BGP2 BY inproc PARALLEL $reducerNum ;
OPTIONAL1 = FOREACH j1 GENERATE $0 AS inproc, $1 AS author, $2 AS booktitle, $3 AS title, $4 AS proc, $5 AS ee, $6 AS page, $7 AS url, $8 AS yr, $10 AS abstract ;

-- SM_Order
SM_Order = ORDER OPTIONAL1 BY yr PARALLEL $reducerNum ;

-- store results into output
STORE SM_Order INTO '$outputData' USING PigStorage(' ') ;
