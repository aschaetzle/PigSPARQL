#!/bin/bash

pig -param inputData='/data/lubm/lubm1000' -param outputData='/data/lubm/lubm1000/partitioned' LUBM_vertical_partitioning.pig
pig -param inputData='/data/lubm/lubm1500' -param outputData='/data/lubm/lubm1500/partitioned' LUBM_vertical_partitioning.pig
pig -param inputData='/data/lubm/lubm2000' -param outputData='/data/lubm/lubm2000/partitioned' LUBM_vertical_partitioning.pig
pig -param inputData='/data/lubm/lubm2500' -param outputData='/data/lubm/lubm2500/partitioned' LUBM_vertical_partitioning.pig
pig -param inputData='/data/lubm/lubm3000' -param outputData='/data/lubm/lubm3000/partitioned' LUBM_vertical_partitioning.pig