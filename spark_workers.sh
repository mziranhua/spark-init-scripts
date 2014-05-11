#!/bin/bash

SPARK_BASE_DIR=/usr/local/share/spark
SPARK_SBIN=$SPARK_BASE_DIR/sbin
PIDS=''

if [ -f $SPARK_BASE_DIR/conf/spark-env.sh  ];then
    source $SPARK_BASE_DIR/conf/spark-env.sh
else
    echo "$SPARK_BASE_DIR/conf/spark-env.sh does not exist. Can't run script."
    exit 1
fi


check_status() {
    
    PIDS=$(ps ax | grep 'org.apache.spark.deploy.worker.Worker' | grep java | grep -v grep | awk '{print $1}')
    
    if [ -n "$PIDS" ]
    then
	return 1
    else
	return 0
    fi
    
}

start() {
    
    check_status
    
    if [ "$?" -ne 0 ]
    then
	echo "Workers already running"
	exit 1
    fi
    
    echo -n "Starting workers ...  "
    for ((i=1; i<=$SPARK_WORKER_INSTANCES; i++)); do
	runuser -c "$SPARK_SBIN/spark-daemon.sh start org.apache.spark.deploy.worker.Worker $i spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT -m $SPARK_WORKER_MEMORY -c $SPARK_WORKER_CORES --webui-port 808$i" spark &>/dev/null
    done
    
    sleep 5
    
    check_status
    
    if [ "$?" -eq 0 ]
    then
	echo "FAILURE"
	exit 1
    fi
    
    echo "SUCCESS"
    exit 0
    
}

stop() {
    
    check_status
    
    if [ "$?" -eq 0 ]
    then
	echo "No workers running ..."
	return 1
    else
	
	echo "Stopping workers ..."
	for ((i=1; i<=$SPARK_WORKER_INSTANCES; i++)); do
	    runuser -c "$SPARK_SBIN/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker $i" spark
	    sleep 4
	done
	echo "done"
	
	return 0
    fi
}

status() {
    
    check_status
    
    if [ "$?" -eq 0 ]
    then
	echo "No workers running"
	exit 1
    else
	echo -n "Workers running: "
	echo $PIDS
	exit 0
    fi
}

case "$1" in
    start)
	start
	;;
    stop)
	stop
	;;
    restart)
	stop
	start
	;;
    status)
	status
	;;
    *)
	echo "Usage: $0 {start|stop|restart|status}"
	exit 1
esac

exit 0