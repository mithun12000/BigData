#!/usr/bin/env python
import re
import sys
import fileinput

lineCount=0
START_PLAY=383
for line in sys.stdin:
	line=line.strip()
	line_lower=line.lower()
	lineCount +=1 
	if(lineCount>=START_PLAY):
		lineStart=lineCount-START_PLAY+1
		if re.search(r'(^|[^a-z0-9])'+re.escape("loue")+r'($|[^a-z0-9])', line_lower): 
			if not 'loue-' in line_lower and not '-loue' in line_lower:
	 			print lineStart, ": ",line
		elif re.search(r'(^|[^a-z0-9])'+re.escape("course")+r'($|[^a-z0-9])', line_lower):
				print lineStart, ": ",line
		elif  re.search(r'(^|[^a-z0-9])'+re.escape("loue")+r'($|[^a-z0-9])', line_lower) and re.search(r'(^|[^a-z0-9])'+re.escape("course")+r'($|[^a-z0-9])', line_lower):
				print lineStart, ": ",line


