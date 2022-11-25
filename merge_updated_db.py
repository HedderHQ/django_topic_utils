# -*- coding: utf-8 -*-

def update_merge_new_transcript_keep_existing(conn):
    for collection_name in ['NYSE', 'NASDAQ' ]:
        agg_results = conn['transcript'][collection_name].aggregate([{"$match" : {"symbol": {"$regex" : "^(?!_)"}} },
                                {"$set": {'insertion_timestamp': datetime.timestamp(datetime.now())}},
                                {"$merge" : 
                                    {
                                        "into":{ "db": 'transcript-dev', "coll": collection_name },
                                        'on'  : ['_id'],
                                        'whenMatched': 'keepExisting',
                                        'whenNotMatched': 'insert'
                                    }
                                },
                                ])
    print(agg_results)
    return agg_results
import argparse

def cmdline_args():
        # Make parser object
    p = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    p.add_argument("mdb_internal_ip",
                   help="internal IP of the mongodb endpoint")
    
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
import os
if __name__ == "__main__":
    print('start checking at pwd=', os.getcwd())
    args = cmdline_args()
    print (args)
    
            
    
    if args.dev_mode:
        print('dev mode')
        mdb_internal_ip = "localhost"
        
    else:
        print('production mode')
        mdb_internal_ip = args.mdb_internal_ip
    from datetime import datetime


    from pymongo import MongoClient
    conn_str = f"mongodb://{mdb_internal_ip}:27017/?directConnection=true&ssl=false"
    conn = MongoClient(conn_str)
    update_merge_new_transcript_keep_existing(conn)