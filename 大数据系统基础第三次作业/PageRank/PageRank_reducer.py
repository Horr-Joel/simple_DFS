#encoding:utf8
import sys
last = None
values = 0.0
alpha = 0.8
N = 4 #网页的数量
for line in sys.stdin:
    data = line.strip().split()
    key,value = data[0],float(data[1])
    if key != last:
        if last:
            values = alpha * values + (1 - alpha) / N
            print values
        last = key
        values = value
    else:
        values += value
if last:
    values = alpha * values + (1 - alpha) / N
    print values