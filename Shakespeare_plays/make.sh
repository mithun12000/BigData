#!/bin/bash
clear

echo "Enter the java file name ";
echo "The java filename as 'UniqueWords.java' ";
read filename;

echo "Enter the directory name to move class files that are generated\n";
echo "Give the directory name as 'class_files'";
read dir;

echo "Enter the directory name to get your output for macbeth";
echo "Give the directory name as 'macbeth'";
read mac;

echo "Enter the directory name to get your output for romeoandjuliet";
echo "Give the directory name as 'romeo'";
read rom;



echo "Enter the jar name to be created";
echo " Give jar name as 'UniqueWords.jar' "; 
read jarname;

if [ -d "$dir" ]; then
\rm -r $dir
mkdir $dir
else 
mkdir $dir
fi

if [ -d "$mac" ]; then
\rm -r $mac
mkdir $mac
else
mkdir $mac 
fi

if [ -d "$rom" ]; then
\rm -r $rom
mkdir $rom
else
mkdir $rom
fi



javac -cp `hadoop classpath` $filename

mv *.class $dir/

jar -cvf $jarname -C $dir/ .

hadoop fs -rm -r -f /user/shavanur/output

hadoop jar $jarname UniqueWords /user/shavanur/Plays/macbeth.txt /user/shavanur/output 

 hadoop fs -copyToLocal /user/shavanur/output/part-r-00000 $mac

hadoop fs -rm -r -f /user/shavanur/output

hadoop jar $jarname UniqueWords /user/shavanur/Plays/romeoAndJuliet.txt /user/shavanur/output

hadoop fs -copyToLocal /user/shavanur/output/part-r-00000 $rom
