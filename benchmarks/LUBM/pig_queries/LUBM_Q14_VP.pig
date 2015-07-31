SET default_parallel $reducerNum ;
SET job.name 'LUBM Q14 ON $inputData' ;

-- load input data
rdf_type = LOAD '$inputData/rdf_type' USING PigStorage('\t') AS (s,o) ;

-- BGP
f0 = FILTER rdf_type BY o == 'ub:UndergraduateStudent' ;
BGP = FOREACH f0 GENERATE s AS x ;


-- store results into output
STORE BGP INTO '$outputData' USING PigStorage('\t') ;
