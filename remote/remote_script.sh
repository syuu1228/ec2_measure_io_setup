#!/bin/bash -e
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install -y wget
cd /etc/apt/sources.list.d/
sudo wget http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list
cd -
sudo apt-get update --allow-insecure-repositories
sudo apt-get install --allow-unauthenticated -y scylla mdadm
sudo /opt/scylladb/scripts/scylla_raid_setup --disks /dev/nvme1n1 --enable-on-nextboot
sudo rm -f /etc/scylla.d/io_properties.yaml
sudo /opt/scylladb/scripts/scylla_io_setup
