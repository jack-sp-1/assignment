#!/bin/bash

#export java_library_path  , hadoop_client_opts
#source any file if required
LOG_DIR = "/some/directory/"


exec 1>${LOG_DIR}/${LOG_FILE} 2>&1

/opt/spark/spark-submit --master yarn --executor-memory 6g --num-executors 200 --totol-executor-cores 1200 --conf deploy-mode "client" final_first.py

if [ $? -eq 0 ]; then
	echo "successful"
	
else
	echo "failed"
fi

echo "job is completed"	
