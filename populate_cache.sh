num_of_caches=2
base_port=21
for (( i = 0; i < num_of_caches; i++ ))
do
    port_num=$((i+base_port))
    echo "Initiating cache with port # $port_num ..."
    sudo python cache.py 300000 $port_num &
    sleep 5
done

