numcaches=100
if [ ! -z "$1" ] ; then
    numcaches=$1
fi
echo "numcaches: $numcaches"
echo "Removing old cache_config.csv."
rm cache_config.csv

for (( i=1; i <= $numcaches; i++ ))
do
    printf -v j "%03d" $i
    echo "$j localhost 60$j localhost 15000000" >> cache_config.csv
done
