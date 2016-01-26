#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Q2.py
#[(0, 2.), (0, 4.), (1, 0.), (1, 10.), (1, 20.)]

from math import isnan
def tokenize(data):
	line=data.split("::")
	if(isnan(float(line[20])) == False):
		return [line[2],float(line[20])]

data = sc.textFile('/review.csv')
map_data = data.map(tokenize)
filter_data = map_data.filter(lambda x: True if x is not None else False)
sumCount = filter_data.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, float(x[1]) + 1),
                             lambda x, y: (x[0] + y[0], float(x[1]) + float(y[1])))
average = sumCount.map(lambda (label, (value_sum, count)): (label, float(value_sum) / float(count)))
average.takeOrdered(10,key=lambda x:-x[1])
