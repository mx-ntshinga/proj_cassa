from __future__ import print_function

import os
import sys
import json
import time
import datetime 
from sys import getsizeof
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement

from datetime import datetime

def bulk_inserts(dataset, session, cassandra_db, iterations):
    try: # file open level

        with open(dataset, mode="r") as jsonDataset_f:
            
            jsonDataset = jsonDataset_f.readlines()

            batch_size = 40.0   # from 23 to 48 for faster batching
            print("\n===============================\nBulkLoad: Tweets from '%s' dataset " % dataset + " >> to >> '%s' Column-family of Cassandra db.\n" % cassandra_db )
            print("  Dataset size = %f MB \n" % (float(os.stat(dataset).st_size)/1000000.0) ) 
            print("  Batch size : %d MB "% batch_size )

            iter_durations = []

            while iterations > 0:
                duration, bulk_count, buffer_size, total_size = 0.0,  0,  0.0,  0.0
                batch = BatchStatement(consistency_level = ConsistencyLevel.ONE)  #QUORUM)

                print("\nIteration: ",iterations)
                for a_tweet in jsonDataset:
                    if iterations<20:
                        pass
                        # print("track .. ", bulk_count )
                    try:
                        jsn_atweet = json.loads(a_tweet) # item in json dataset
                        if 'retweeted_status' in jsn_atweet and 'quoted_status' in jsn_atweet['retweeted_status']:
                            jsn_atweet["retweeted_status"]["quoted_status"] = str(jsn_atweet['retweeted_status']['quoted_status'])

                        timestamp_ms =  int(jsn_atweet['timestamp_ms'])
                        datetime_x  = datetime.utcfromtimestamp( timestamp_ms//1000 ).replace( microsecond=timestamp_ms%1000*1000 ).strftime('20%y-%m-%d %H:%M:%S%z')
                        jsn_atweet['created_at'] = datetime_x
                        jsn_atweet['tweet_id'] = bulk_count+1

                        for user_e in jsn_atweet['user']:
                            jsn_atweet ['user_'+user_e] = jsn_atweet['user'][user_e]
                        del jsn_atweet['user']

                        json_stm = json.dumps( jsn_atweet ).replace("'", "`") # Replace single quotes, not supported by cql json insert

                        buffer_size += (float(sys.getsizeof( json_stm )) / 1000000.0)
                        total_size  += (float(sys.getsizeof( json_stm )) / 1000000.0)

                        sys.stdout.write("  Bulking Tweet [%i] t-size [%f MB]...  \r" % (bulk_count+1, total_size) )
                        sys.stdout.flush()
                        batch.add(SimpleStatement("INSERT INTO tweets JSON %s"), (json_stm, ))

                        # Write in batches, to avoid overwhelming cluster heap size and latency
                        if int(buffer_size) == int(batch_size):
                            try:
                                start_time = time.time()
                                results = session.execute_async ( batch )
                                rows = list(results.result())
                                duration += time.time()-start_time
                                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                                buffer_size = 0.0

                            except Exception as exp_t: 
                                print("execute error : " + exp_t.__str__() + "\n " + json.dumps(jsn_atweet, indent=4))
                                break

                        bulk_count += 1

                    except Exception as exp_t: 
                        print("\nStatement error : " + exp_t.__str__()) 
                        break

                # execute remaining statements in batch
                # rows = list(results.result())

                start_time = time.time()
                results = session.execute_async ( batch )
                rows = list(results.result())
                duration += time.time()-start_time
                print("  Insert Tweet [%i] t-size [%f MB] ...  \r" % (bulk_count, total_size) )
                print("  Elapsed time : ", duration)

                iter_durations.append( duration )
                iterations -= 1
                
                if not iterations == 0:
                    session.execute("TRUNCATE TABLE tweets;")

            avg = sum(iter_durations) / float( max(len(iter_durations), 1))
            with open("results/iterations.json","a+") as bulk_f:
                json.dump({"dataset":dataset, "insertBulkBatch": iter_durations,"average":avg}, bulk_f)  # indent=4, separators=(',', ': ')
                bulk_f.write("\n")

            print("%i iterations: %s " % (iterations,str(iter_durations)) ) 

    except KeyboardInterrupt as keyb:
        print("\nOperation cancelled!")

    except Exception as error:
        print("\n" + error.__str__())
