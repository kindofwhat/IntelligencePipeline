#!/bin/bash


topics=(document-representation-ingestion document-representation-event metadata-event datarecord-event datarecord-consolidated chunk)

service=`docker ps | grep broker | awk '{ print $11}'`
echo $service


function docker_exec_topic() {
    docker exec $1 kafka-topics --$2 --topic $3 $4 --zookeeper zookeeper:2181
}

for topic in "${topics[@]}"; do docker_exec_topic $service delete $topic; done
for topic in "${topics[@]}"; do docker_exec_topic $service create $topic "--partitions 1 --replication-factor 1 --if-not-exists"; done

