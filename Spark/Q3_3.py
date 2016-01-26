#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Q3_3.py
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
#  spark-submit --master yarn --name FindSimilairBusiness ../Q3_3.py -b /business_id.txt -m /file.txt -o /Matrix


import os
import sys
import getopt

from pyspark import SparkContext,SparkConf

class Q1:
		
	def __init__(self,argv):	
		try:
			self.data=[]
			opts, args = getopt.getopt(argv,"hb:m:o:",["file1=","file2=","ofile="])
		except getopt.GetoptError:
			print 'Q3_3.py -b <business_ids> -m <matrix> -o <outputfile>'
			sys.exit(2)
		for opt, arg in opts:
			if opt == '-h':
				print 'Q3_3.py -b <business_ids> -m <matrix> -o <outputfile>'
				sys.exit(0)
			elif opt in ("-b", "--file1"):
				self.infile1 = arg
			elif opt in ("-m", "--file2"):
				self.infile2 = arg	
			elif opt in ("-o", "--ofile"):
				self.outfile = arg
		
	def tokenize_business(self,text):
		return [text,None]			
				
	def tokenize_matrix(self,text):
		line = text.split('\t')
		key=line[0].rstrip()
		val=""
		if(len(line) > 1):
			valSet=line[1].split(' ')
			for k in range(0,len(valSet)):
				val=val+str(valSet[k].split(':')[0])+','				
			
		return [key,val.rstrip(',')]
	
	def tokenize_result(self,text):
		
		return (str(text[0])+'\t'+str(text[1][1]))
			
			
def main():
	sc = SparkContext()
	obj = Q1(sys.argv[1:])
	os.popen("hdfs dfs -rmr -f "+obj.outfile)
	
	# File1 has business_id's corresponding to the user
	# File2 is the item similarity co-occurence matrix
	
	File1 = sc.textFile(obj.infile1)
	File2 = sc.textFile(obj.infile2)
	
	File1_filter = File1.map(obj.tokenize_business).filter(lambda x: True if x is not None else False)
	File2_filter = File2.map(obj.tokenize_matrix).filter(lambda x: True if x is not None else False)
	
	result = File1_filter.join(File2_filter).map(obj.tokenize_result).filter(lambda x: True if x is not None else False)
	result.saveAsTextFile(obj.outfile)
	
	return 0

if __name__ == "__main__":
   main()
