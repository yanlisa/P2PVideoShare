numcaches=5
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
    echo "$j ${iplocal} 60$j ${ippublic} 50000 30" >> cache_config.csv
done
