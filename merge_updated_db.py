# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 13:28:25 2022

@author: chanh
"""

from datetime import datetime

mdb_internal_ip = "127.0.0.1"
from pymongo import MongoClient
conn_str = f"mongodb://{mdb_internal_ip}:27017/?directConnection=true&ssl=false"
conn = MongoClient(conn_str)


conn.transcript.NYSE.aggregate([{"$match" : {"symbol" : {"$regex": "^AA\w+"} }},
                                {"$set": {'insertion_time': datetime.timestamp(datetime.now())}},
                                {"$out" : 'temp'}
                                ])


conn.transcript.NYSE.aggregate([{"$match" : {"symbol" : {"$regex": "^AA\w+"} }},
                                {"$set": {'insertion_timestamp': datetime.timestamp(datetime.now())}},
                                {"$merge" : 
                                     {
                                         "into":'temp',
                                         'on'  : '_id',
                                         'whenMatched': 'keepExisting',
                                         'whenNotMatched': 'insert'
                                     }
                                },
                                ])

for collection_name in ['NYSE', 'NASDAQ' ]:
    conn.transcript.NYSE.aggregate([{"$match" : {"symbol": {"$regex" : "^[^_]w*"}} },
                                {"$set": {'insertion_timestamp': datetime.timestamp(datetime.now())}},
                                {"$merge" : 
                                     {
                                         "into":{ "db": 'transcript-dev', "coll": collection_name },
                                         'on'  : '_id',
                                         'whenMatched': 'keepExisting',
                                         'whenNotMatched': 'insert'
                                     }
                                },
                                ])
conn['transcript-dev'].NYSE.count_documents({})




for collection_name in ['NYSE', 'NASDAQ' ]:
    conn.transcript.NYSE.aggregate([{"$match" : {"symbol": {"$regex" : "^[^_]w*"}} },
                                {"$set": {'insertion_timestamp': datetime.timestamp(datetime.now())}},
                                {"$merge" : 
                                     {
                                         "into":{ "db": 'transcript-dev', "coll": collection_name },
                                         'on'  : '_id',
                                         'whenMatched': 'keepExisting',
                                         'whenNotMatched': 'insert'
                                     }
                                },
                                ])
for collection_name in ['NYSE', 'NASDAQ' ]:
    conn.transcript[collection_name].aggregate([{"$match" : {"symbol": {"$regex" : "^[^_]w*"}} },
                                {'$unset' : '_id'},
                                {"$set": {'insertion_timestamp': datetime.timestamp(datetime.now())}},
                                {"$merge" : 
                                     {
                                         "into":{ "db": 'transcript-dev', "coll": collection_name },
                                         'on'  : "_id",
                                         'whenMatched': 'merge',
                                         'whenNotMatched': 'insert'
                                     }
                                },
                                ])
for collection_name in ['NYSE', 'NASDAQ' ]:
    conn.transcript[collection_name + "_bak"].aggregate([{"$match" : {"symbol": {"$regex" : "^[^_]w*"}} },
                                {'$unset' : '_id'},
                                {"$set": {'insertion_timestamp': datetime.timestamp(datetime.now())}},
                                {"$merge" : 
                                     {
                                         "into":{ "db": 'transcript-dev', "coll": collection_name },
                                         'on'  : ['content'],
                                         'whenMatched': 'merge',
                                         'whenNotMatched': 'insert'
                                     }
                                },
                                ])
        
from copy import deepcopy
for collection_name in ['NYSE', 'NASDAQ' ]:
    cursor = conn['transcript-dev'][collection_name + '_bak'].find({})
    for doc in cursor:
        doc_clone = doc.deepcopy()
        doc_clone.pop('_id')
        doc_clone.pop('_id')
        conn['transcript-dev'][collection_name]
        pass