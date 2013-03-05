pids=`ps -C python -o pid,cmd | grep "python ../../cache.py" | awk '{print $1}'`
#processes=`ps -C python -o pid,cmd | grep "python cache.py"`
# might use the processes var in order to print statements regarding who
# is being killed
for pid in $pids
do
    echo Killing cache with PID $pid ...
    sudo kill $pid
done
