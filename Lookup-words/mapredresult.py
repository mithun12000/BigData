#!/usr/bin/python
import re
import sys

#print "Enter a file name:",
#filename = raw_input()
num1=0
num2=0
num3=0
f1=open(sys.argv[2], 'w+')

with open(sys.argv[1]) as myile:
	print >>f1,"########################LOUE WORD#####################################"
        for num1, line in enumerate(myile, 1):
                line = line.strip()
		line_lower = line.lower()

        
                if re.search(r'(^|[^a-z0-9])'+re.escape("loue")+r'($|[^a-z0-9])', line_lower):
                        print >>f1, line
	print >>f1,"----------------------------------------------------------------------"
	print >>f1,"\n"
	print >>f1,"The total number of lines in which word 'loue' appears is: ",num1
	print >>f1,"----------------------------------------------------------------------"
	print >>f1,"\n\n"

with open(sys.argv[1]) as myile:
	print >>f1,"########################COURSE WORD###################################"
	for line in myile:
		line = line.strip()
                line_lower = line.lower()

                
                if re.search(r'(^|[^a-z0-9])'+re.escape("course")+r'($|[^a-z0-9])', line_lower):
			num2 +=1
                        print  >>f1,line
	print >>f1,"----------------------------------------------------------------------"
        print >>f1,"\n"
	print >>f1,"The total number of lines in which word 'course' appears is: ",num2
	print >>f1,"----------------------------------------------------------------------"
        print >>f1,"\n\n"


with open(sys.argv[1]) as myile:
	print >>f1,"########################BOTH COURSE AND LOUE WORD#####################################"
	for line in myile:
		line = line.strip()
                line_lower = line.lower()
		if re.search(r'(^|[^a-z0-9])'+re.escape("course")+r'($|[^a-z0-9])', line_lower) and re.search(r'(^|[^a-z0-9])'+re.escape("loue")+r'($|[^a-z0-9])', line_lower):
			num3 +=1
			print  >>f1,line
	print >>f1,"--------------------------------------------------------------------------------------"
	print >>f1,"\n"
        print >>f1,"The total number of lines in which word 'loue' and 'course' appears is: ",num3
	print >>f1,"--------------------------------------------------------------------------------------"
        print >>f1,"\n\n"
