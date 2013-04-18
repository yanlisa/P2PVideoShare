rm log/*
echo "Initiating tracker..."
python tracker.py 8081 > log/tracker.txt &

