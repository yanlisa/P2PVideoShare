num_of_caches=2
base_port=49152 # this is the first of the private ports, so we won't disrupt
                # operations using reserved ports.
if [ ! -d "cache"$i ]; then
    mkdir "caches"
fi
cd "caches"
for (( i = 1; i <= num_of_caches; i++ ))
do
    echo "Initiating cache with port # $i ..."
    if [ ! -d "cache"$i ]; then
        mkdir "cache"$i
    fi
    cd "cache"$i
    sudo python ../../cache.py $i & > log.txt
    cd ..
    sleep 5
done

