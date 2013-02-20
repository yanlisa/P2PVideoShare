num_of_caches=2
base_port=49152 # this is the first of the private ports, so we won't disrupt
                # operations using reserved ports.
for (( i = 0; i < num_of_caches; i++ ))
do
    port_num=$((i+base_port))
    echo "Initiating cache with port # $port_num ..."
    if [ ! -d "cache"$i ]; then
        mkdir "cache"$i
    fi
    cd "cache"$1
    sudo python ../cache.py 300000 $port_num & > log.txt
    cd ..
    sleep 5
done

