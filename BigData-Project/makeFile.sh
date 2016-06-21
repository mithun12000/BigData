#!/bin/bash

ch="Y"
pwds=`pwd`
while [ $ch == "Y" ] 
do
clear
echo "#####################################################################################################################"
echo "                                                                                                                     "
echo "                                                  Project : Bigdata                                                  "
echo "                                                  Dr Chuck Cartledge                                                 "
echo "                                  By: Srinivas Havanur, Kevin Garner, Prasanna Sajjan                                "
echo "#####################################################################################################################"

if [ -f *.log ]
then 
	\rm *.log
fi

echo "1. Cardiovascular stress test(93015)";
echo "2. Generate count of medicare and pharmaceutical records before and after the join(93015)."
echo "3. Extra Credit. Electrocardiogram report(93010)";
echo "4. Generate count of medicare and pharmaceutical records before and after the join(93010)."

echo "Please enter your choice";

read choice;


case $choice in
1) echo "Output for Cardiovascular stress test with hcpcs code 93015";

	echo "Enter a directory name to generate output file to get statistics for virginia";
	echo "Give the directory name as '93015_OUTPUT'";
	read dir1;
	if [ -d "$dir1" ]; then 
	\rm -r $dir1
	mkdir $dir1
	else
	mkdir $dir1
	fi
	
	/opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output

	pig project_update_93015.pig 

	/opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output/part-r-00000 $pwds/$dir1

	echo "The output file for Cardiovascular stress test with code 93015 is generated in the following path: $pwds/$dir1" 
	;;
2) echo "Output to get the count of records before and after the join with hcpcs code 93015";

	echo "Enter a directory name to generate the count of medicare before join";
	echo "Give the directory name as 'MEDICARE_COUNT_93015'";
	read dir3;
	echo "Enter a directory name to generate the count of pharmaceutical payments before join"
	echo "Give the directory name as 'PHARMA_COUNT_93015'";
	read dir4;
	echo "Enter a directory name to generate the count of records after joining both medicare and pharmaceutical payments";
	echo "Give the directory name as 'JOIN_COUNT_93015'";
	read dir5;

	if [ -d "$dir3" ]; then
        \rm -r $dir3
        mkdir $dir3
        else
        mkdir $dir3
        fi

	if [ -d "$dir4" ]; then
        \rm -r $dir4
        mkdir $dir4
        else
        mkdir $dir4
        fi

	if [ -d "$dir5" ]; then
        \rm -r $dir5
        mkdir $dir5
        else
        mkdir $dir5
        fi


	/opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output1
	/opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output2
	/opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output3

	pig project_count_93015.pig

	/opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output1/part-r-00000 $pwds/$dir3
	/opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output2/part-r-00000 $pwds/$dir4
	/opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output3/part-r-00000 $pwds/$dir5

	echo "The output files for count before and after join are generated in the following path:  $pwds/$dir3";
	echo "The output files for count before and after join are generated in the following path:  $pwds/$dir4";
	echo "The output files for count before and after join are generated in the following path:  $pwds/$dir5";

	;;
	


	

3) echo "Graduate part execution starts here";
	echo "Enter a directory name to generate output for Extra credit. Unique code in this case is 93010(Electrocardiogram report)";
	echo "Give the directory name as '93010_OUTPUT'";
	read dir2;
	if [ -d "$dir2" ]; then
        \rm -r $dir2
        mkdir $dir2
        else
        mkdir $dir2
        fi

	/opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output

	pig project_update_93010.pig

	/opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output/part-r-00000 $pwds/$dir2

	 echo "The output file for Extra credit is generated in the following path: $pwds/$dir2";

	;;

4) echo "Output to get the count of records before and after the join with hcpcs code 93010";

        echo "Enter a directory name to generate the count of medicare before join";
        echo "Give the directory name as 'MEDICARE_COUNT_93010'";
        read dir6;
        echo "Enter a directory name to generate the count of pharmaceutical payments before join"
        echo "Give the directory name as 'PHARMA_COUNT_93010'";
        read dir7;
        echo "Enter a directory name to generate the count of records after joining both medicare and pharmaceutical payments";
        echo "Give the directory name as 'JOIN_COUNT_93010'";
        read dir8;

        if [ -d "$dir6" ]; then
        \rm -r $dir6
        mkdir $dir6
        else
        mkdir $dir6
        fi

        if [ -d "$dir7" ]; then
        \rm -r $dir7
        mkdir $dir7
        else
        mkdir $dir7
        fi

        if [ -d "$dir8" ]; then
        \rm -r $dir8
        mkdir $dir8
        else
        mkdir $dir8
        fi


        /opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output1
        /opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output2
        /opt/hadoop-2.5.1/bin/hadoop fs -rm -r -f /user/shavanur/output3

        pig project_count_93010.pig

        /opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output1/part-r-00000 $pwds/$dir6
        /opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output2/part-r-00000 $pwds/$dir7
        /opt/hadoop-2.5.1/bin/hadoop fs -copyToLocal /user/shavanur/output3/part-r-00000 $pwds/$dir8

        echo "The output files for count before and after join are generated in the following path:  $pwds/$dir6";
        echo "The output files for count before and after join are generated in the following path:  $pwds/$dir7";
        echo "The output files for count before and after join are generated in the following path:  $pwds/$dir8";

        ;;


esac
echo "Press Y if you want to continue generating Undergrad/Graduate part data extraction";
echo "Press N if you want to exit";
echo "Do you want to continue or exit?";
read ch;
done
