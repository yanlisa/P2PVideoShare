num_of_users=$2
base_port=49152 # this is the first of the private ports, so we won't disrupt
                # operations using reserved ports.
if [ ! -d "users" ]; then
    mkdir "users"
fi
cd "users"
for (( i = 1; i <= num_of_users; i++ ))
do
    echo "Initiating user # $i ..."
    if [ ! -d "user_"$1"_"$i ]; then
        mkdir "user_"$1"_"$i
    fi
    cd "user_"$1"_"$i
    rm -r video*
    python ../../user.py $1 "user-"$1"-"$i > ../../log/user_$1_$i.txt &
    cd ".."
    sleep 1
done
cd ".."

