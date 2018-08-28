from __future__ import print_function

import os
import sys
import json
import time
import datetime 
from sys import getsizeof
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

def insertOne(dataset, session, cassandra_db, iterations):
    print("\n===============================\nInsertONE: Tweets from '%s' dataset " % dataset + " >> to >> '%s' Column-family of Cassandra db.\n" % cassandra_db )
    print("  Dataset size = %f MB \n" % (float(os.stat(dataset).st_size)/1000000.0) )

    try:
        with open(dataset, mode="r") as jsonDataset_f:
            jsonDataset = jsonDataset_f.readlines()
            iter_durations = []

            while iterations > 0:
                lt_count = 0
                duration = 0.0
                total_size = 0.0
                print("\nIteration: ",iterations)
                for a_tweet in jsonDataset:
                    try:
                        json_stm = json.loads(a_tweet) # item in json dataset
                        total_size  += (float(sys.getsizeof( json_stm )) / 1000000.0)
                        # print("Size : ", float(sys.getsizeof( json_stm )))

                        sys.stdout.write("  Insert Tweet [%i] t-size [%f MB] ...  \r" % (lt_count, total_size) )
                        sys.stdout.flush()

                        # REFORMATING while DECODING JSON values for cassandra's accepted data types
                        try:
                            if 'retweeted_status' in json_stm and 'quoted_status' in json_stm['retweeted_status']:
                                    json_stm["retweeted_status"]["quoted_status"] = str(json_stm['retweeted_status']['quoted_status'])

                            timestamp_ms =  int(json_stm['timestamp_ms'])
                            json_stm['created_at']  = datetime.utcfromtimestamp( timestamp_ms//1000).replace(microsecond=timestamp_ms%1000*1000).strftime('20%y-%m-%d %H:%M:%S%z')
                            json_stm['tweet_id'] = lt_count+1

                            json_stm_ex = json.dumps( json_stm ).replace("'", "`") # Replace sinlge quotes, not supported by cql json insert

                            start_time = time.time()
                            session.execute ( "INSERT INTO tweets JSON '" + json_stm_ex + "'" )
                            end_time = time.time()
                            session.execute ( "DELETE FROM tweets WHERE tweet_id = " + str(json_stm["tweet_id"]) + "" )

                        except Exception as exp_t:
                            stm_error = "\nStatement execute error : " + exp_t.__str__() + "\n " + json.dumps(json_stm, indent=4)
                            print(stm_error)
                            break

                        elapsed_time = end_time - start_time
                        duration += elapsed_time

                        # avg_latency = elapsed_time / lt_count         # Record METRICS [ LATENCY,  THROUGHPUT, AVERAGES ]
                        # recordMetrics( elapsed_time, item_size = float(sys.getsizeof(a_tweet)) /1000000.0)
                        lt_count += 1

                    except KeyboardInterrupt as keyb:
                        print("\n\nOperation cancelled!")
                        break

                    except Exception as error:
                        print("\n" + error.__str__())

                print("  Insert Tweet [%i] t-size [%f MB] ...  \r" % (lt_count, total_size) )
                print("  Elapsed_time : ", duration ) 

                iter_durations.append( duration )
                iterations -= 1

            print("%i iterations: %s " % (iterations, str(iter_durations)) )
            avg = sum (iter_durations) / float( max(len(iter_durations), 1) )
            with open ("results/iterations.json","a+") as bulk_f:
                json.dump({"dataset":dataset, "insertOne": iter_durations,"average":avg}, bulk_f)
                bulk_f.write("\n")

    except Exception as except_inst:
        print("\nDataset Insert error : " + except_inst.__str__())

    # global avg_laten
    # global lt_size