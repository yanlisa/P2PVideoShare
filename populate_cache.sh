num_of_caches=10
base_port=49152 # this is the first of the private ports, so we won't disrupt
                # operations using reserved ports.
cd "config"
./edit_cache_config.sh
cd ".."

rm log/*
rm -r caches
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
    #python ../../cache.py $i > ../../log/cache_$i.txt &
    if [ i == 1 ]; then
	    python ../../cache.py $i &
    else
    	python ../../cache.py $i > ../../log/cache_$i.txt &
    fi
	
    cd ..
    sleep .1
done
cd ".."
