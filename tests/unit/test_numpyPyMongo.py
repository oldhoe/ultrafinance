# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 18:12:34 2015

@author: pchaosgit
"""
import random

import numpy
from pymongo import MongoClient

NUM_BATCHES = 3500
BATCH_SIZE = 1000
# 3500 batches * 1000 per batch = 3.5 million records

c = MongoClient("localhost")
collection = c.testdb.collection

def save():
	for i in range(NUM_BATCHES):
		stuff = [ ]
		for j in range(BATCH_SIZE):
			record = dict(x1=random.uniform(0, 1),
						  x2=random.uniform(0, 2),
						  x3=random.uniform(0, 3),
						  x4=random.uniform(0, 4),
						  x5=random.uniform(0, 5)
					 )
			stuff.append(record)
		collection.insert_many(stuff)

def load():
	num = collection.count()
	arrays = [ numpy.zeros(num) for i in range(5) ]

	for i, record in enumerate(collection.find()):
		for x in range(5):
			arrays[x][i] = record["x%i" % x+1]

	for array in arrays: # prove that we did something...
		print(numpy.mean(array))