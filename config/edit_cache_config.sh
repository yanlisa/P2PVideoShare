numcaches=100
if [ ! -z "$1" ] ; then
    numcaches=$1
fi
echo "numcaches: $numcaches"
echo "Removing old cache_config.csv."
rm cache_config.csv

iplocal=`./ip_local.sh`
ippublic=`./ip_public.sh`
for (( i=1; i <= $numcaches; i++ ))
do
    printf -v j "%03d" $i
    echo "$j 0.0.0.0 60$j ${ippublic} 15000000" >> cache_config.csv
done
