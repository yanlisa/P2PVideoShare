if [ ! -d "server" ]; then
    mkdir "server"
fi
cd "server"
echo "Initiating server..."
rm server_load.txt
python ../server.py  > ../log/server.txt &
cd ".."
