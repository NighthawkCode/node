#!/bin/bash

# makesure we are running on a clean system
killall -q nodesrv 
killall -q prod_app
killall -q cons_app

rm -f *_log.txt

if [ ! -f build/apps/nodesrv ]; then
   echo "nodesrv binary could not be found"
   exit 1
fi 

if [ ! -f build/test/cons_app ]; then
   echo "prod binary could not be found"
   exit 1
fi 

if [ ! -f build/test/prod_app ]; then
   echo "prod binary could not be found"
   exit 1
fi 


# Launch our core
echo "Launching nodesrv..."
build/apps/nodesrv > server_log.txt 2>&1 & CORE=$!
echo "Sleeping for 1 second to ensure the server is up"
sleep 1
echo "Launching producer..."
build/test/prod_app > prod_log.txt 2>&1 & PROD=$!
echo "Launching consumer 1..."
build/test/cons_app > cons1_log.txt 2>&1 & CONS1=$!
echo "Launching consumer 2..."
build/test/cons_app > cons2_log.txt 2>&1 & CONS2=$!
echo "Launching consumer 3..."
build/test/cons_app > cons3_log.txt 2>&1 & CONS3=$!

GOOD=1

echo "Waiting for the test to complete..."

wait $PROD
if [ "$?" -ne "0" ]; then
  echo "Error on producer exit"
  GOOD=0
fi

wait $CONS1
if [ "$?" -ne "0" ]; then
  echo "Error on consumer 1 exit"
  GOOD=0
fi

wait $CONS2
if [ "$?" -ne "0" ]; then
  echo "Error on consumer 2 exit"
  GOOD=0
fi

wait $CONS3
if [ "$?" -ne "0" ]; then
  echo "Error on consumer 3 exit"
  GOOD=0
fi

if [ "$GOOD" -eq "1" ]; then 
  echo "SUCCESS on test"
else 
  echo "FAILURE on test"
fi


# clean the nodecore if it is still running
killall -q nodesrv
