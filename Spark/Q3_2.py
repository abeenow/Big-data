#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Q3_2.py
#  
#  Copyright 2015 Abinav <abinav@BIGDEN>
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
#  spark-submit --master local --name FilterBusinessId ../Q3_2.py -i review.csv -o run1 -u sCiWf7Kai0t-x25CAz2DEQ
#  spark-submit --master yarn --name FilterBusinessId ../Q3_2.py -i /review.csv -o /run1 -u sCiWf7Kai0t-x25CAz2DEQ


import os
import sys
import getopt

from pyspark import SparkContext,SparkConf

class Q1:
		
	def __init__(self,argv):	
		try:
			opts, args = getopt.getopt(argv,"hi:o:u:",["ifile=","ofile=","userid="])
		except getopt.GetoptError:
			print 'Q3_2.py -i <inputfile> -o <outputfile> -u <User_Id>'
			sys.exit(2)
		for opt, arg in opts:
			if opt == '-h':
				print 'Q3_2.py -i <inputfile> -o <outputfile> -u <User_Id>'
				sys.exit(0)
			elif opt in ("-i", "--ifile"):
				self.infile = arg
			elif opt in ("-o", "--ofile"):
				self.outfile = arg
			elif opt in ("-u", "--userid"):
				self.uid = arg
				
	def tokenize(self,text):
		line=text.split("::")
		if(line[8] == self.uid and line[20] == '4'):
			return line[2]
			
def main():
	sc = SparkContext()
	obj = Q1(sys.argv[1:])
	os.popen("hadoop dfs -rmr -f "+obj.outfile)
	text = sc.textFile(obj.infile)
	words = text.map(obj.tokenize)
	loc = words.filter(lambda x: True if x is not None else False)
	loc.saveAsTextFile(obj.outfile)
	return 0

if __name__ == "__main__":
   main()
