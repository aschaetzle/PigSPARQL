#!/usr/bin/perl
use strict;
use warnings;

if ( @ARGV < 4 )
{
  print "Informatin missing!\n";
  print "usage: <datasetSize> <inputPath> <outputPath> <reducerNum> \n\n";
  exit(1);
}

my $datasetSize = $ARGV[0];
my $inputPath = $ARGV[1];
my $outputPath = $ARGV[2];
my $reducerNum = $ARGV[3];


open(my $out, ">>", "results/Pig_BSBM_eval_VP_replicated_$datasetSize.txt") or die "Can't open OutputFile: $!";
print $out "#####################################################\n";
print $out "# START OF EVALUATION SEQUENCE: ",timestamp(time)," #\n";
print $out "#####################################################";

######
# Q1 #
######    
open(my $runs, "<", "parameter_values/BSBM_Q1_$datasetSize.txt") or die "Can't open Parameter File: $!";
print $out "\n\n### Q1:\n";
my $count = 0;
while(my $line = <$runs>) {
   my @params = split(' ', $line);
   if($params[0] !~ "#+") {

       $count++;
       my $query = "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q1_rep_$count' -param reducerNum='$reducerNum' -param ProductType='$params[0]' -param ProductFeature1='$params[1]' -param ProductFeature2='$params[2]' -param x=$params[3] ../pig_queries/BSBM_Q1_VP_replicated.pig";
	   
	   my $startTime = time();
	   my $startTimeStamp = timestamp($startTime);
	   print "\n# Q1: $query\n";
	   print $out "\n Q1: $query\n";
	   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
	   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

	   system($query);

	   my $endTime = time();
	   my $endTimeStamp = timestamp($endTime);
	   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
	   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
	   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
	   print $out " Execution Time: ",$endTime - $startTime," s\n";

	   sleep(30);
   }
}
close $runs or die "$runs: $!";

######
# Q2 #
######    
open(my $runs, "<", "parameter_values/BSBM_Q2_$datasetSize.txt") or die "Can't open Parameter File: $!";
print $out "\n\n### Q2:\n";
my $count = 0;
while(my $line = <$runs>) {
   my @params = split(' ', $line);
   if($params[0] !~ "#+") {

       $count++;
	   my $query = "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q2_rep_$count' -param reducerNum='$reducerNum' -param ProductXYZ='$params[0]' ../pig_queries/BSBM_Q2_VP_replicated.pig";
	   
	   my $startTime = time();
	   my $startTimeStamp = timestamp($startTime);
	   print "\n# Q2: $query\n";
	   print $out "\n Q2: $query\n";
	   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
	   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

	   system($query);

	   my $endTime = time();
	   my $endTimeStamp = timestamp($endTime);
	   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
	   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
	   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
	   print $out " Execution Time: ",$endTime - $startTime," s\n";

	   sleep(30);
   }
}
close $runs or die "$runs: $!";

######
# Q3 #
######    
open(my $runs, "<", "parameter_values/BSBM_Q3_$datasetSize.txt") or die "Can't open Parameter File: $!";
print $out "\n\n### Q3:\n";
my $count = 0;
while(my $line = <$runs>) {
   my @params = split(' ', $line);
   if($params[0] !~ "#+") {

       $count++;
	   my $query = "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q3_rep_$count' -param reducerNum='$reducerNum' -param ProductType='$params[0]' -param ProductFeature1='$params[1]' -param ProductFeature2='$params[2]' -param x=$params[3] -param y=$params[4] ../pig_queries/BSBM_Q3_VP_replicated.pig";
	   
	   my $startTime = time();
	   my $startTimeStamp = timestamp($startTime);
	   print "\n# Q3: $query\n";
	   print $out "\n Q3: $query\n";
	   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
	   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

	   system($query);

	   my $endTime = time();
	   my $endTimeStamp = timestamp($endTime);
	   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
	   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
	   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
	   print $out " Execution Time: ",$endTime - $startTime," s\n";

	   sleep(30);
   }
}
close $runs or die "$runs: $!";

######
# Q5 #
######    
open(my $runs, "<", "parameter_values/BSBM_Q5_$datasetSize.txt") or die "Can't open Parameter File: $!";
print $out "\n\n### Q5:\n";
my $count = 0;
while(my $line = <$runs>) {
   my @params = split(' ', $line);
   if($params[0] !~ "#+") {

       $count++;
	   my $query = "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q5_rep_$count' -param reducerNum='$reducerNum' -param ProductXYZ='$params[0]' ../pig_queries/BSBM_Q5_VP_replicated.pig";
	   
	   my $startTime = time();
	   my $startTimeStamp = timestamp($startTime);
	   print "\n# Q5: $query\n";
	   print $out "\n Q5: $query\n";
	   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
	   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

	   system($query);

	   my $endTime = time();
	   my $endTimeStamp = timestamp($endTime);
	   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
	   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
	   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
	   print $out " Execution Time: ",$endTime - $startTime," s\n";

	   sleep(30);
   }
}
close $runs or die "$runs: $!";

######
# Q6 #
######    
open(my $runs, "<", "parameter_values/BSBM_Q6_$datasetSize.txt") or die "Can't open Parameter File: $!";
print $out "\n\n### Q6:\n";
my $count = 0;
while(my $line = <$runs>) {
   my @params = split(' ', $line);
   if($params[0] !~ "#+") {

       $count++;
	   my $query = "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q6_rep_$count' -param reducerNum='$reducerNum' -param word1='$params[0]' ../pig_queries/BSBM_Q6_VP_replicated.pig";
	   
	   my $startTime = time();
	   my $startTimeStamp = timestamp($startTime);
	   print "\n# Q6: $query\n";
	   print $out "\n Q6: $query\n";
	   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
	   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

	   system($query);

	   my $endTime = time();
	   my $endTimeStamp = timestamp($endTime);
	   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
	   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
	   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
	   print $out " Execution Time: ",$endTime - $startTime," s\n";

	   sleep(30);
   }
}
close $runs or die "$runs: $!";

######
# Q7 #
######    
open(my $runs, "<", "parameter_values/BSBM_Q7_$datasetSize.txt") or die "Can't open Parameter File: $!";
print $out "\n\n### Q7:\n";
my $count = 0;
while(my $line = <$runs>) {
   my @params = split(' ', $line);
   if($params[0] !~ "#+") {

       $count++;
	   my $query = "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q7_rep_$count' -param reducerNum='$reducerNum' -param ProductXYZ='$params[0]' -param currentDate='$params[1]' ../pig_queries/BSBM_Q7_VP_replicated.pig";
	   
	   my $startTime = time();
	   my $startTimeStamp = timestamp($startTime);
	   print "\n# Q7: $query\n";
	   print $out "\n Q7: $query\n";
	   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
	   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

	   system($query);

	   my $endTime = time();
	   my $endTimeStamp = timestamp($endTime);
	   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
	   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
	   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
	   print $out " Execution Time: ",$endTime - $startTime," s\n";

	   sleep(30);
   }
}
close $runs or die "$runs: $!";

######
# Q8 #
######    
open(my $runs, "<", "parameter_values/BSBM_Q8_$datasetSize.txt") or die "Can't open Parameter File: $!";
print $out "\n\n### Q8:\n";
my $count = 0;
while(my $line = <$runs>) {
   my @params = split(' ', $line);
   if($params[0] !~ "#+") {

       $count++;
	   my $query = "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q8_rep_$count' -param reducerNum='$reducerNum' -param ProductXYZ='$params[0]' ../pig_queries/BSBM_Q8_VP_replicated.pig";
	   
	   my $startTime = time();
	   my $startTimeStamp = timestamp($startTime);
	   print "\n# Q8: $query\n";
	   print $out "\n Q8: $query\n";
	   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
	   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

	   system($query);

	   my $endTime = time();
	   my $endTimeStamp = timestamp($endTime);
	   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
	   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
	   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
	   print $out " Execution Time: ",$endTime - $startTime," s\n";

	   sleep(30);
   }
}
close $runs or die "$runs: $!";

#######
# Q10 #
#######    
open(my $runs, "<", "parameter_values/BSBM_Q10_$datasetSize.txt") or die "Can't open Parameter File: $!";
print $out "\n\n### Q10:\n";
my $count = 0;
while(my $line = <$runs>) {
   my @params = split(' ', $line);
   if($params[0] !~ "#+") {

       $count++;
	   my $query = "pig -param inputData='$inputPath' -param outputData='$outputPath/$datasetSize/Q10_rep_$count' -param reducerNum='$reducerNum' -param ProductXYZ='$params[0]' -param currentDate='$params[1]' ../pig_queries/BSBM_Q10_VP_replicated.pig";
	   
	   my $startTime = time();
	   my $startTimeStamp = timestamp($startTime);
	   print "\n# Q10: $query\n";
	   print $out "\n Q10: $query\n";
	   print "# STARTING EVALUATION: $startTime -> $startTimeStamp\n\n";
	   print $out " Starting Evaluation: $startTime -> $startTimeStamp\n";

	   system($query);

	   my $endTime = time();
	   my $endTimeStamp = timestamp($endTime);
	   print "# EVALUATION FINISHED: ", "$endTime -> $endTimeStamp\n";
	   print $out " Evaluation finished: ", "$endTime -> $endTimeStamp\n";
	   print "# EXECUTION TIME: ",$endTime - $startTime," s\n\n";
	   print $out " Execution Time: ",$endTime - $startTime," s\n";

	   sleep(30);
   }
}
close $runs or die "$runs: $!";


sub timestamp {
   (my $sec,my $min,my $hour,my $mday,my $mon,my $year) = localtime(shift);
   $year += 1900; #Jahr zählt von 1970
   $mon +=1; #Monat 0-11
   my $stamp = sprintf("%02d:%02d:%02d-%02d.%02d.%04d",$hour,$min,$sec,$mday,$mon,$year);
   return $stamp;
}
