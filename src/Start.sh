rm site1.txt
rm site2.txt
rm site3.txt
rm site4.txt
rm site5.txt
rm site6.txt
rm site7.txt
rm site8.txt
rm site9.txt
rm site10.txt

stdbuf -oL python Site.py 1 9090 > site1.txt &
stdbuf -oL python Site.py 2 9091 > site2.txt &
stdbuf -oL python Site.py 3 9092 > site3.txt &
stdbuf -oL python Site.py 4 9093 > site4.txt &
stdbuf -oL python Site.py 5 9094 > site5.txt &
stdbuf -oL python Site.py 6 9095 > site6.txt &
stdbuf -oL python Site.py 7 9096 > site7.txt &
stdbuf -oL python Site.py 8 9097 > site8.txt &
stdbuf -oL python Site.py 9 9098 > site9.txt &
stdbuf -oL python Site.py 10 9099 > site10.txt &