if [ ! -d "server" ]; then
    mkdir "server"
fi
cd "server"
echo "Initiating server..."
rm -r server_load_*
#python ../server.py  > ../log/server.txt &
python ../server.py &
cd ".."
