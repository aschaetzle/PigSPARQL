%default reducerNum '1';

-- load input data
indata = LOAD '$inputData' USING PigStorage(' ') AS (s,p,o,dot) ;

no_dot = FOREACH indata GENERATE s,p,o ;
no_duplicates = DISTINCT no_dot PARALLEL $reducerNum ;

-- store into output
STORE no_duplicates INTO '$outputData' USING PigStorage(' ') ;