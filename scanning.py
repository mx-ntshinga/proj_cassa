from __future__ import print_function

import json
import time
import datetime
import os
import sys
from sys import getsizeof
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import Statement
from datetime import datetime 

def scanAll(session, iterations, jsn_dataset):

    print("\n===============================\nFull Scanning of column-family: Tweets")
    print("* Condition : token(tweet_id) >= -9223372036854775808")
    print("* Scanning split into parallel steps of %i's pages.\n" % 100) 

    # Latency : Time it takes to scan x tweet(s)
    # Throughput :  Number (and size in MB) of tweets scanned in x timeframe

    try:
        scan_length = 50000000
        Statement.fetch_size = scan_length
        Statement.ConsistencyLevel = ConsistencyLevel.ONE  # more than one response ) to ensure no data is missing from querying replicas

        iter_durations = []
        while iterations>0:
            # session.execute ("--request-timeout=6000;")
            next_range = 0l
            curr_range = -9223372036854775808
            step = 100000000000000000
            limit = 9223372036854775808

            results_len = 0
            duration = 0.0

            print("\nIteration: ",iterations)

            # while curr_range < 9223372036854775808: 
            while curr_range <= limit: #and results_len <= scan_length and :
                next_range = curr_range + step 
                if (curr_range + step)>limit:
                    next_range = limit-1
                statement_str = "SELECT token(tweet_id) FROM tweets WHERE token(tweet_id) >= %i AND token(tweet_id) < %i ; " % (curr_range, next_range)
                start = time.time()
                results = session.execute_async ( statement_str ) 
                results_len += len(list(results.result())) 
                duration = time.time() - start   # record event time x

                curr_range += step

            print("  Scanned Rows [ %i ] .... \r" % (results_len) )
            print("  Elapsed time : %s seconds" % duration)
            iter_durations.append( duration )
            iterations -= 1

        avg = sum(iter_durations) / float( max(len(iter_durations), 1)) 
        with open("results/iterations.json","a+") as bulk_f:
            json.dump({"dataset":jsn_dataset, "scanAll": iter_durations,"average":avg}, bulk_f)
            bulk_f.write("\n")

        print("%i iterations: %s " % (iterations, str(iter_durations)) )

    except Exception as error:
        print(error.__str__())
    except KeyboardInterrupt as keyb:
        print("\n\nOperation cancelled!")