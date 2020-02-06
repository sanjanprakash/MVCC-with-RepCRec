rm nohup.out
source ./Start.sh
nohup stdbuf -oL python2 TransactionManager.py &
echo $! > save_pid.txt
sleep 2
python2 Simulator.py ../data/Test1.txt
kill -9 `cat save_pid.txt`
source ./Stop.sh