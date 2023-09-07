#!/bin/bash

# makesure we are running on a clean system
killall -q nodemaster
killall -q prod
killall -q cons_app

nodemaster &

rm -f *_log.txt

if [ ! -f ${HOME}/verdant/dev/build/third_party/vstack/node/test/prod ]; then
   echo "prod binary could not be found"
   exit 1
fi

if [ ! -f ${HOME}/verdant/dev/build/third_party/vstack/node/test/cons_app ]; then
   echo "cons_app binary could not be found"
   exit 1
fi

sleep 1

echo "Launching producer..."
${HOME}/verdant/dev/build/third_party/vstack/node/test/prod -m 100 &> ${HOME}/verdant/dev/third_party/vstack/node/test/prod_log.txt & PROD=$!

echo "Launching consumer app..."
${HOME}/verdant/dev/build/third_party/vstack/node/test/cons_app -m 350 &> ${HOME}/verdant/dev/third_party/vstack/node/test/cons_app_log.txt & CONS_APP=$!

GOOD=1
sleep 13

echo "Launching producer again..."
${HOME}/verdant/dev/build/third_party/vstack/node/test/prod -m 100 &> ${HOME}/verdant/dev/third_party/vstack/node/test/prod_log_1.txt & PROD=$!

echo "Waiting for the test to complete..."
sleep 17

if [ "$?" -ne "0" ]; then
  echo "Error on producer exit"
  GOOD=0
fi

if [ "$?" -ne "0" ]; then
  echo "Error on consumer_app exit"
  GOOD=0
fi

if [ "$GOOD" -eq "1" ]; then
  echo "SUCCESS on test"
else
  echo "FAILURE on test"
fi

# clean the nodecore if it is still running
killall -q nodemaster
