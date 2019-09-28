#!/bin/bash

for i in {0..10}
do
	 echo -$i
	 ./groupmessenger2-grading.osx  ./app-debug.apk  > file-$i.txt
	 
done