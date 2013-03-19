num_of_caches=3
base_port=49152 # this is the first of the private ports, so we won't disrupt
                # operations using reserved ports.
if [ ! -d "caches" ]; then
    mkdir "caches"
fi
cd "caches"
for (( i = 1; i <= num_of_caches; i++ ))
do
    echo "Initiating cache with id # $i ..."
    if [ ! -d "cache"$i ]; then
        mkdir "cache"$i
    fi
    cd "cache"$i
    rm -r video*
    python ../../cache.py $i > ../../log/cache_$i.txt &
    cd ..
    sleep 1
done
cd ".."
