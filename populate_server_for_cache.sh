if [ ! -d "server_for_cache" ]; then
    mkdir "server_for_cache"
fi
cd "server_for_cache"
echo "Initiating server..."
python ../server_for_cache.py  > ../log/server_for_cache.txt &
cd ".."

