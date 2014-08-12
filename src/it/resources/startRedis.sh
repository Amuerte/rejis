#! /bin/bash

nohup redis-server &
nohup redis-server --port 6389 --slaveof 127.0.0.1 6379 &
nohup redis-server --port 6399 --slaveof 127.0.0.1 6379 &