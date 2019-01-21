start_tm=`date +%s%N`;


awk -F "[,\\\\-  ]" '
BEGIN {
    maxlen=0;
    maxtime=0;
    lensum=0;
    numsum=0;
    timesum=0;
    count=0
}
{
    lensum+=$16;
    numsum+=$14;
    timesum+=$15;
    month[$7]++;
    day[$8]++;
    count++;
    maxlen=($16 > maxlen?$16:maxlen);
    maxtime=($15 >maxtime?$15:maxtime)
}
END{
    print "month  count";
    for(i in month)
    {

        print " "i"     "month[i];

    };
    print "day  count";
    for(j in day)
    {

        print " "j"     "day[j] ;

    };
    print "lenavg="lensum/count;
    print "numavg="numsum/count ;
    print "timeavg="timesum/count ;
    print "maxlen is "maxlen;
    print "maxtime is "maxtime;
}' dataset.csv >> result.txt

end_tm=`date +%s%N`;
use_tm=`echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}'`
echo "usedtime is "$use_tm >>result.txt
