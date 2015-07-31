#!/usr/bin/perl
use strict;
use warnings;

if ( @ARGV < 3 )
{
  print "Informatin missing!\n";
  print "usage: <datasetSize> <inputPath> <outputPath> \n\n";
  exit(1);
}

my $datasetSize = $ARGV[0];
my $inputPath = $ARGV[1];
my $outputPath = $ARGV[2];


my @queries = (
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q1' -param reducerNum='27' ../pig_queries/LUBM_Q1_VP.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q1_rep' -param reducerNum='27' ../pig_queries/LUBM_Q1_VP_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q2' -param reducerNum='27' ../pig_queries/LUBM_Q2_VP_MJ.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q2_rep' -param reducerNum='27' ../pig_queries/LUBM_Q2_VP_MJ_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q3' -param reducerNum='27' ../pig_queries/LUBM_Q3_VP.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q3_rep' -param reducerNum='27' ../pig_queries/LUBM_Q3_VP_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q4' -param reducerNum='27' ../pig_queries/LUBM_Q4_VP_MJ.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q5' -param reducerNum='27' ../pig_queries/LUBM_Q5_VP.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q5_rep' -param reducerNum='27' ../pig_queries/LUBM_Q5_VP_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q6' -param reducerNum='27' ../pig_queries/LUBM_Q6_VP.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q7' -param reducerNum='27' ../pig_queries/LUBM_Q7_VP_MJ.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q7_rep' -param reducerNum='27' ../pig_queries/LUBM_Q7_VP_MJ_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q8' -param reducerNum='27' ../pig_queries/LUBM_Q8_VP_MJ.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q9' -param reducerNum='27' ../pig_queries/LUBM_Q9_VP_MJ.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q10' -param reducerNum='27' ../pig_queries/LUBM_Q10_VP.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q10_rep' -param reducerNum='27' ../pig_queries/LUBM_Q10_VP_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q11' -param reducerNum='27' ../pig_queries/LUBM_Q11_VP.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q11_rep' -param reducerNum='27' ../pig_queries/LUBM_Q11_VP_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q12' -param reducerNum='27' ../pig_queries/LUBM_Q12_VP_MJ.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q12_rep' -param reducerNum='27' ../pig_queries/LUBM_Q12_VP_MJ_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q13' -param reducerNum='27' ../pig_queries/LUBM_Q13_VP.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q13_rep' -param reducerNum='27' ../pig_queries/LUBM_Q13_VP_replicated.pig",
   "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q14' -param reducerNum='27' ../pig_queries/LUBM_Q14_VP.pig",
);


open(my $out, ">>", "results/Pig_LUBM_eval_VP_$datasetSize.txt") or die "Can't open OutputFile: $!";
print $out "#####################################################\n";
print $out "# START OF EVALUATION SEQUENCE: ",timestamp(time)," #\n";
print $out "#####################################################";


for(my $i = 0; $i < @queries; $i++) {

   my $startTime = time();
   my $startTimeStamp = timestamp($startTime);
   print "\n\n# QUERY $i: $queries[$i]\n";
   print $out "\n\n Query $i: $queries[$i]\n";
   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

   system($queries[$i]);

   my $endTime = time();
   my $endTimeStamp = timestamp($endTime);
   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
   print $out " Execution Time: ",$endTime - $startTime," s\n";
   
   sleep(60);
   
}

print $out "\n\n\n";
close $out or die "$out: $!";


sub timestamp {
   (my $sec,my $min,my $hour,my $mday,my $mon,my $year) = localtime(shift);
   $year += 1900; #Jahr zählt von 1970
   $mon +=1; #Monat 0-11
   my $stamp = sprintf("%02d:%02d:%02d-%02d.%02d.%04d",$hour,$min,$sec,$mday,$mon,$year);
   return $stamp;
}
