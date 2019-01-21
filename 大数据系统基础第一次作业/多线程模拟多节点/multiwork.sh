start_tm=`date +%s%N`;


for ((i=1;i<=5;i++))
do
{

awk -F "[,\\\\-  ]" '
BEGIN {
    maxlen=0;
    maxtime=0;
    lensum=0;
    numsum=0;
    timesum=0;
    count=0;
}
{
    lensum+=$16;
    numsum+=$14;
    timesum+=$15;
    count++;
    maxlen=($16 > maxlen?$16:maxlen);
    maxtime=($15 >maxtime?$15:maxtime);
}
END{
    print "lenavg,"lensum/count;
    print "numavg,"numsum/count ;
    print "timeavg,"timesum/count ;
    print "maxlen,"maxlen;
    print "maxtime,"maxtime;
    print "count,"count;
}' data${i}.csv >> result${i}.txt
}&

done
wait
{
    maxlen_=0;
    maxtime_=0;
    lenavg_=0;
    numavg_=0;
    timeavg_=0;
    count_=0
    for((j=1;j<=5;j++))
    {
        count[j]=$(sed -n '6p' result${j}.txt | awk -F , '{print $2}' );
        count_+=count[j];

        lenavg[j]=$(sed -n '1p' result${j}.txt | awk -F , '{print $2}' );
        
        numavg[j]=$(sed -n '2p' result${j}.txt | awk -F , '{print $2}' );
        timeavg[j]=$(sed -n '3p' result${j}.txt | awk -F , '{print $2}' );

        maxlen[j]=$(sed -n '4p' result${j}.txt | awk -F , '{print $2}' );
        maxlen1=${maxlen[j]};
        if [ `expr $maxlen_ \> $maxlen1` -eq 0 ];then
            maxlen_=${maxlen[j]};
        fi;


        maxtime[j]=$(sed -n '5p' result${j}.txt | awk -F , '{print $2}' );
        maxtime1=${maxtime[j]};
        if [ `expr $maxtime_ \> $maxtime1` -eq 0 ];then
            maxtime_=${maxtime[j]};
        fi;

    }

    echo "maxlength is "$maxlen_ >>final_result.txt;
    echo "maxtime is "$maxtime_ >>final_result.txt;
: '
    for((k=1;k<=5;k++))
    {
        count1=${count[k]};
        weight=`echo "scale=2;$count1/$count_" |bc`
        lenavg1=${lenavg[k]};
        numavg1=${numavg[k]};
        timeavg1=${timeavg[k]};
        lenavg_=`echo "scale=2;$lenavg_+$weight*$lenavg1" |bc`
        numavg_=`echo "scale=2;$numavg_+$weight*$numavg1" |bc`
        timeavg_=`echo "scale=2;$timeavg_+$weight*$timeavg1" |bc`
    }
    echo "lenavg is "$lenavg_ >>final_result.txt;
    echo "numavg is "$numavg_ >>final_result.txt;
    echo "timeavg is "$timeavg_ >>final_result.txt;
'

}

end_tm=`date +%s%N`;
use_tm=`echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}'`
echo "usedtime is "$use_tm >>final_result.txt
