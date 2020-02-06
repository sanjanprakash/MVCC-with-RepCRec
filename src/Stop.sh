ps -ef | grep 'python Site.py' | grep -v grep | awk '{print $2}' | xargs kill -9
pkill -f TransactionManager.py