# -*- coding: utf-8 -*-
"""
Created on Wed Oct 19 15:30:02 2022

@author: chanh
"""
import datetime
import bson
import pymongo
import argparse




def get_symbol(symbol, collection_name, update_if_not_exist=True, year_since = None):
    """
    given a symbol, update the cache results of that particular results

    Returns
    -------
    None.

    """
    # get cached symbol
    if year_since is None:
        year_since = 0
    data = list(client[db][collection_name].find(
                {"symbol":symbol, 
                 'trend_cache': None,
                 'year' : {'$gte': year_since}
                 },
                {
                    
                }
            )
        )
   
    return data
    
def query_symbol(symbol, year_since = 0):
    import requests
    response = requests.get(f"http://{api_base_url}/api/v1/company/{symbol}/trend/?year_since={year_since}")
    return response.json()

def update_symbol_topic_trend(list_of_records_to_update, collection_name):
    
    for i in list_of_records_to_update:
        trend_cache = {
                "updated_at_ts": datetime.datetime.timestamp(datetime.datetime.now()),
                "words" : i["words"],
                "scores": i['scores']
                       }
        client[db][collection_name].update_one(
                upsert = False, 
                filter = {"_id" : bson.objectid.ObjectId(i['_id'])},
                update = {"$set" : {"trend_cache": trend_cache}}
                )
    

def update_pipeline(symbol, collection_name):
    symbol_data = get_symbol(symbol, collection_name)
    if len(symbol_data) == 0:
        return
    year_to_start = min(i['year'] for i in symbol_data if "trend_cache" not in i)
    unupdated_data = query_symbol(symbol, year_since = year_to_start)
    update_symbol_topic_trend(unupdated_data, collection_name)

def update_all(continue_from = (None, None)):
    found = False
    if continue_from == (None, None):
        found = True
    for collection_name in ["NYSE", "NASDAQ"]:
        
        symbol_list = client[db][collection_name].distinct('symbol')
        
        for sym in symbol_list:
            if collection_name == continue_from[0] and sym == continue_from[1]:
                found = True
            if found:    
                print(datetime.datetime.now(), 'updating', sym, "from ", collection_name)
                update_pipeline(sym, collection_name)
                print(datetime.datetime.now(), 'updated', sym, "from ", collection_name)

def test_random(n=30, year_to_start =2018):
    import numpy as np
    for collection_name in ["NYSE", "NASDAQ"]:
        
        symbol_list = client[db][collection_name].distinct('symbol')
        symbol_list=np.random.choice(symbol_list, n, replace=False)
        for sym in symbol_list:
            print(datetime.datetime.now(), 'requesting', sym, "from ", collection_name)
            query_symbol(sym, year_since = year_to_start)
            print(datetime.datetime.now(), 'received', sym, "from ", collection_name)
    
def cmdline_args():
        # Make parser object
    p = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    p.add_argument("mdb_internal_ip",
                   help="desc")
    p.add_argument("api_base_url",
                   help="desc")
    # p.add_argument("required_int", type=int,
    #                help="req number")
    p.add_argument("--dev_mode", action="store_true", default=False,
                    help="enable to use dev mode parameters, masking other manual inputs")
    p.add_argument("--test", action="store_true", default=False,
                    help="enable to use dev mode parameters, masking other manual inputs")
    p.add_argument("-v", "--verbosity", type=int, choices=[0,1,2], default=0,
                   help="increase output verbosity (default: %(default)s)")
                 
    # group1 = p.add_mutually_exclusive_group(required=True)
    # group1.add_argument('--enable',action="store_true")
    # group1.add_argument('--disable',action="store_false")

    return(p.parse_args())

if __name__ == "__main__":
    args = cmdline_args()
    print (args)
    
    
    if args.dev_mode:
        mdb_internal_ip = "localhost"
        api_base_url = 'localhost:9001'
    else:
        mdb_internal_ip = args.mdb_internal_ip
        api_base_url = args.api_base_url
    CONNECTION_STRING = f"mongodb://{mdb_internal_ip}:27017/?directConnection=true&ssl=false"
    client = pymongo.MongoClient(CONNECTION_STRING)
    db = 'transcript-dev'
    if args.test:
        
        test_random(10)
    else:
        update_all()
    #update_all(continue_from = ("NASDAQ","WKME") )
    #update_pipeline('AAPL', "NASDAQ")
"""
CVE
import requests

temp = requests.get("http://localhost:9001/api/v1/company/AAPL/trend/?year_since=2020")
"""

