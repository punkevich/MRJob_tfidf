#!/usr/bin/env python3

# output descending sort
import os
path = 'output/'
fileList = os.listdir(path)
arr = []
for i in fileList:
	file = open(os.path.join(path + i), 'r')
	for line in file:
		temp = line.split('\t')
		x = temp[0]
		y = temp[1]
		arr.append([x, y])	

arr = sorted(arr, key=lambda item: item[1], reverse=True)
for item in arr:
	print(item[0], item[1])
