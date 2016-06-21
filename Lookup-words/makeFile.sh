#!/bin/bash
pwds=`pwd`
clear
echo "#####################################################################################################################"
echo "														 	   " 
echo "						   	Assignment 2: Bigdata					           "
echo " 						 	 Dr Chuck Cartledge 					 	   " 
echo "														 	   " 
echo "#####################################################################################################################"
 

echo "Enter the python mapper file"
echo "The file name is 'mapper.py'"
read mapperfile

echo "Enter the python reducer file"
echo "The file name is 'reducer.py'"
read reducerfile

echo "Enter the directory name to get a output file"
read dir

if [ -d "$dir" ]; then
\rm -r $dir
mkdir $dir
else
mkdir $dir
fi

hadoop fs -rm -r -f /user/shavanur/output
hadoop jar /opt/hadoop-2.5.1/share/hadoop/tools/lib/hadoop-streaming-2.5.1.jar -D mapred.map.tasks=1 -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator -D mapred.text.key.comparator.options=-n  -mapper  mapper.py -reducer reducer.py -file $mapperfile -file $reducerfile -input "/user/shavanur/Plays/midsummerNight'sDream.txt" -output "/user/shavanur/output"

hadoop fs -copyToLocal /user/shavanur/output/part-00000 $dir

clear
echo "#####################################################################################################################"
echo "                                                                                                                     "
echo "                                                  Assignment 2: Bigdata                                              "
echo "                                                   Dr Chuck Cartledge                                                "
echo "                                                                                                                     "
echo "#####################################################################################################################"
echo "Do you want to generate a 'results.txt' file to segregate the 3 conditions? Press y or n"
read choice

if [ $choice == "y" ]
then
	if [ -f $dir/results.txt ]
	then
		\rm $dir/results.txt
	fi 
        python mapredresult.py $dir/part-00000 $dir/results.txt
echo "The output files 'part-00000' and 'results.txt' are generated and copied in: $pwds/$dir"  
else
	if [ -f $dir/results.txt ]
        then
                \rm $dir/results.txt
        fi
echo "The output file 'part-00000' is generated and copied in: $pwds/$dir"
fi

