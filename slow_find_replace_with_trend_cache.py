# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 15:24:38 2022

@author: chanh
"""

from datetime import datetime

mdb_internal_ip = "127.0.0.1"
from pymongo import MongoClient
conn_str = f"mongodb://{mdb_internal_ip}:27017/?directConnection=true&ssl=false"
conn = MongoClient(conn_str)


# from copy import deepcopy
# for collection_name in ['NYSE', 'NASDAQ' ]:
#     cursor = conn['transcript-dev'][collection_name + '_bak'].find({})
#     for doc in cursor:
#         doc_clone = deepcopy(doc)
#         doc_clone.pop('_id')
#         doc_clone.pop('trend_cache')
#         q_results = list(conn['transcript-dev'][collection_name].find(doc_clone))
#         if len(q_results) != 1:
#             print(len(q_results))
        
            
#             conn['transcript-dev']['debug'].insert_many(q_results, upsert=True)
            
            
#%% dedup pipeline



for collection_name in ['NYSE' , 'NASDAQ']:
  #  c_need_update = conn["transcript-dev"][collection_name].find({"trend_cache" : {"exists" : False} })
    c_updated = conn["transcript-dev"][collection_name + "_bak"].find(
        {"trend_cache" : {"$exists" : True}, 
         "symbol": {"$regex" : "^[C-Z]+"}
         }
        )
    for c in c_updated:
        res = conn["transcript-dev"][collection_name].update_many(
            filter = {'content' : c['content']},
            update = {'$set': {"trend_cache" : c['trend_cache']}}
            )
        print(c['symbol'], res.modified_count, res.matched_count)