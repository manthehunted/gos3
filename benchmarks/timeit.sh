#!/bin/bash
set -e

bucket=$1
prefix=$2

for i in $(seq 0 2); do
	echo "python"
	# FIXME: has to be in virtualenv
	time python ../py/bench_async.py $bucket $prefix > /dev/null
	echo $(ls *json.gz| wc -l)
	rm *json.gz
	echo

	echo "golang"
	time ../s3 $bucket $prefix > /dev/null
	echo $(ls *json.gz| wc -l)
	rm *json.gz
	echo
done;

'
python

real    0m24.294s
user    0m2.193s
sys     0m0.535s
203

golang

real    0m1.145s
user    0m0.337s
sys     0m0.223s
203

python

real    0m23.245s
user    0m2.193s
sys     0m0.529s
203

golang

real    0m1.243s
user    0m0.333s
sys     0m0.265s
203

python

real    0m22.886s
user    0m2.211s
sys     0m0.560s
203

golang

real    0m1.692s
user    0m0.341s
sys     0m0.241s
203
'
