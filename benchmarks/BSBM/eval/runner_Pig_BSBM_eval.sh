#!/bin/bash

./Pig_BSBM_eval_VP.pl 1000K /data/bsbm/berlin_1000K_VP_m PigSPARQL/results/BSBM 27
./Pig_BSBM_eval_VP.pl 1500K /data/bsbm/berlin_1500K_VP_m PigSPARQL/results/BSBM 27
./Pig_BSBM_eval_VP.pl 2000K /data/bsbm/berlin_2000K_VP_m PigSPARQL/results/BSBM 27
./Pig_BSBM_eval_VP.pl 2500K /data/bsbm/berlin_2500K_VP_m PigSPARQL/results/BSBM 27
./Pig_BSBM_eval_VP.pl 3000K /data/bsbm/berlin_3000K_VP_m PigSPARQL/results/BSBM 27

./Pig_BSBM_eval_VP_replicated.pl 1000K /data/bsbm/berlin_1000K_VP_m PigSPARQL/results/BSBM 27
./Pig_BSBM_eval_VP_replicated.pl 1500K /data/bsbm/berlin_1500K_VP_m PigSPARQL/results/BSBM 27
./Pig_BSBM_eval_VP_replicated.pl 2000K /data/bsbm/berlin_2000K_VP_m PigSPARQL/results/BSBM 27
./Pig_BSBM_eval_VP_replicated.pl 2500K /data/bsbm/berlin_2500K_VP_m PigSPARQL/results/BSBM 27
./Pig_BSBM_eval_VP_replicated.pl 3000K /data/bsbm/berlin_3000K_VP_m PigSPARQL/results/BSBM 27