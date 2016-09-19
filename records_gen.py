#!/bin/python
import random
courses = ['COL100', 'COL352', 'COL733', 'COL776']
N = 100000
avg = dict([course,0] for course in courses)
f = open('record_dump.dat','w')
for i in xrange(1,N+1):
  for course in courses:
    score = random.randint(0,100)
    f.write("{} {} {}\n".format(i,course,score))
    avg[course] += float(score)/float(N)
print avg
