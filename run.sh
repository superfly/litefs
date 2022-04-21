#!/bin/bash

echo "mounting litefs..."
litefs -url http://liteserver.internal:8080 /data &
sleep 5
echo "litefs mounted"

echo "starting litefs-demo"
litefs-demo -dsn /data/db
