num_of_users=3
base_port=49152 # this is the first of the private ports, so we won't disrupt
                # operations using reserved ports.
if [ ! -d "users" ]; then
    mkdir "users"
fi
cd "users"
for (( i = 1; i <= num_of_users; i++ ))
do
    echo "Initiating user # $((num_of_users + i)) ..."
    if [ ! -d "user"$((num_of_users + i)) ]; then
        mkdir "user"$((num_of_users + i))
    fi
    cd "user"$((i+num_of_users))
    rm -r video*
    python ../../user.py hyunah2 > ../../log/user_$((i+num_of_users)).txt &
    cd ".."
    sleep 1
done
cd ".."
