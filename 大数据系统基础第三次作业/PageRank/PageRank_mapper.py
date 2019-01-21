#encoding:utf8
import sys

#每一行格式为前面是Mi的转置，最后是vi
for line in sys.stdin:
    data = line.strip().split()
    Mi = data[:-1]
    Vi = float(data[-1])
    for i in range(len(Mi)):
        value = float(Mi[i])*Vi
        print "%d %f" % (i+1,value)