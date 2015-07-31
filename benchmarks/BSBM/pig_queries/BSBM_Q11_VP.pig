SET default_parallel $reducerNum ;
SET job.name 'BSBM Q11 ON $inputData' ;

-- load input data
indata = LOAD '$inputData/inputData' USING PigStorage('\t') AS (s,p,o) ;

-- BGP
f0 = FILTER indata BY s == '$OfferXYZ' ;
BGP1 = FOREACH f0 GENERATE p AS property, o AS hasValue ;

-- BGP
f0 = FILTER indata BY o == '$OfferXYZ' ;
BGP2 = FOREACH f0 GENERATE s AS isValueOf, p AS property ;

-- UNION
UNION1 = UNION ONSCHEMA BGP1, BGP2 ;

-- store results into output
STORE UNION1 INTO '$outputData' USING PigStorage('\t') ;
