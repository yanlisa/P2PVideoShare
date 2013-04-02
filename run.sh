rm -r log/*
./populate_tracker.sh
sleep .3
./populate_server.sh
sleep .3
./populate_cache.sh
sleep 1
./populate_users.sh
