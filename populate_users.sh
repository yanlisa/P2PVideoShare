num_of_users=5
if [ ! -z "$1" ] ; then
    num_of_users=$1
fi
rm log/*
rm -r users
if [ ! -d "users" ]; then
    mkdir "users"
fi
cd "users"
for (( i = 1; i <= num_of_users; i++ ))
do
    echo "Initiating user # $i ..."
    if [ ! -d "user_"$i ]; then
        mkdir "user_"$i
    fi
    cd "user_"$i
    rm -r video*
    #python ../../user.py > ../../log/user_$i.txt &
    python ../../user.py ../../development.ini > /dev/null &
    cd ".."
done
cd ".."
