#!/bin/sh

for i in `seq ${1} ${2}`; do
	./crawler.py $i &
	sleep 1
done
