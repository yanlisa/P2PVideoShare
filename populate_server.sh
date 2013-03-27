if [ ! -d "server" ]; then
    mkdir "server"
fi
cd "server"
echo "Initiating server..."
python ../server.py  > ../log/server.txt &
cd ".."
