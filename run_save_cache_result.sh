#!/bin/sh
while :
    do
        /home/peter/.pyenv/shims/python save_cache_results.py 10.104.0.2 167.71.205.189:9000 && echo success all && sleep 86400 
        sleep 60
    done