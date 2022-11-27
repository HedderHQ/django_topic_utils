#!/bin/sh
while :
    do
        /home/peter/.pyenv/shims/python merge_updaed_db.py 10.104.0.2 && echo success all && sleep 86400 
        sleep 60
    done