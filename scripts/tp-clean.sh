#!/bin/bash

echo "disable 'entries'; disable 'hash'; disable 'images'; drop 'entries'; drop 'hash'; drop 'images'" | hbase shell

hadoop fs -rm -r -f '/texaspete/data/*' '/texaspete/ev/*' '/texaspete/map/*' '/texaspete/json/*'
