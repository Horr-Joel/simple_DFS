#!/bin/bash
start_tm=`date +%s%N`;
count=20
for i in `seq 1 $count`
do
	echo "NO.$i"
	cat V.txt
	python PageRank_pretreatment.py
	cat MTV.txt | python PageRank_mapper.py | sort -k1,1 |python PageRank_reducer.py >V.txt
done
end_tm=`date +%s%N`;
use_tm=`echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}'`
echo "usedtime is "$use_tm