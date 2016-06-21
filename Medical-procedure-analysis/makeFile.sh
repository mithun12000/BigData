#!/bin/bash

ch="Y"
pwds=`pwd`
while [ $ch == "Y" ] 
do
clear
echo "#####################################################################################################################"
echo "                                                                                                                     "
echo "                                                  Assignment 3: Bigdata                                              "
echo "                                                   Dr Chuck Cartledge                                                "
echo "                                                                                                                     "
echo "#####################################################################################################################"

if [ -f *.log ]
then 
	\rm *.log
fi

echo "1. Average cost of Cardio vascular stress test based on Virginia Practitioner.";
echo "2. Average cost of Cardio vascular stress test across different states in US";

echo "Press 1 or 2 for your choice";

read choice;


case $choice in
1) echo "Virginia stats execution starts here";

	echo "Enter a directory name to generate output file to get statistics for virginia";
	echo "Give the directory name as 'Practitioner_stats'";
	read dir1;
	if [ -d "$dir1" ]; then 
	\rm -r $dir1
	mkdir $dir1
	else
	mkdir $dir1
	fi
	
	/opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output

	pig practitioner.pig

	/opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output/part-r-00000 $pwds/$dir1

	echo "The output file for Virginia Practitioner stats are generated in the following path: $pwds/$dir1" 
	;;
2) echo "Stats accross different states starts here";
	echo "Enter a directory name to generate an output file to get statistics of different states in US";
	echo "Give the directory name as 'States_stats'";
	read dir2;
	if [ -d "$dir2" ]; then
        \rm -r $dir2
        mkdir $dir2
        else
        mkdir $dir2
        fi

	/opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output

	pig states.pig

	/opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output/part-r-00000 $pwds/$dir2

	 echo "The output file for different states stats are generated in the following path: $pwds/$dir2";

	;;

esac
echo "Press Y if you want to continue generating Practitioner/States part data extraction";
echo "Press N if you want to exit";
echo "Do you want to continue or exit?";
read ch;
done

