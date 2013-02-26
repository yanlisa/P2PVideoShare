num_of_users=10
base_port=49152 # this is the first of the private ports, so we won't disrupt
                # operations using reserved ports.
if [ ! -d "users" ]; then
    mkdir "users"
fi
cd "users"
for (( i = 1; i <= num_of_users; i++ ))
do
    echo "Initiating user # $i ..."
    if [ ! -d "user"$i ]; then
        mkdir "user"$i
    fi
    cd "user"$i
    sudo python ../../user.py OnePiece575 & > log.txt
    cd ..
    sleep 5
done

