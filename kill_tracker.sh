pids=`ps -af | grep "python tracker.py" | awk '{print $2}'`
#processes=`ps -C python -o pid,cmd | grep "python cache.py"`
# might use the processes var in order to print statements regarding who
# is being killed
for pid in $pids
do
    kill $pid
done
