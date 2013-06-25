rm -r log/*
./populate_tracker.sh
sleep .3
curl localhost:8081/req/RESET
./populate_server.sh
