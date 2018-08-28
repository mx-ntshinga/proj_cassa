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
from cassandra.query import Statement

def insertOneIndexed(dataset, session, cassandra_db): 
    print("===============================\nInsertOne (Indexed with (created_at, lang)): Tweet from '%s' dataset " % dataset + " >> to >> '%s' Column-family of Cassandra db.\n" % cassandra_db )
    print("  Dataset size = %f MB \n" % (float(os.stat(dataset).st_size)/1000000.0) ) 

    date_index = "CREATE INDEX IF NOT EXISTS date_index ON tweets (created_at);"
    lang_index = "CREATE INDEX IF NOT EXISTS lang_index ON tweets (lang);"
    followers_index = "CREATE INDEX IF NOT EXISTS followers_count ON tweets (user.followers_count);"
    location_index = "CREATE INDEX IF NOT EXISTS location ON tweets (user.location);"
    friends_index = "CREATE INDEX IF NOT EXISTS friends_count ON tweets (user.friends_count);"

    Statement.ConsistencyLevel = ConsistencyLevel.ALL  # more than one response ) to ensure all replicas are aware of this query, and no data is missing when querying coordinator.

    session.execute(date_index) 
    session.execute(lang_stm)
    session.execute(location_index)
    session.execute(followers_index)
    session.execute(friends_index)

    try: 
        with open(dataset, mode="r") as jsonDataset_f:
            jsonDataset = jsonDataset_f.readlines()
            lt_count = 1
            iter_durations = []

            while iterations>0: 
                duration = 0.0
                print("\nIteration: ",iterations)
                for a_tweet in jsonDataset:
                    try:
                        json_stm = json.loads(a_tweet) # item in json dataset
                        total_size  += (float(sys.getsizeof( json_stm )) / 1000000.0)

                        sys.stdout.write("  Insert Tweet [%i] t-size [%0.2f MB] ...  \r" % (lt_count, total_size) ) 
                        sys.stdout.flush()

                        # REFORMATING while DECODING JSON values for cassandra's accepted data types
                        try:
                            if 'retweeted_status' in json_stm and 'quoted_status' in json_stm['retweeted_status']:
                                    json_stm["retweeted_status"]["quoted_status"] = str(json_stm['retweeted_status']['quoted_status'])

                            timestamp_ms =  int(json_stm['timestamp_ms'])
                            json_stm['created_at']  = datetime.utcfromtimestamp( timestamp_ms//1000).replace(microsecond=timestamp_ms%1000*1000).strftime('20%y-%m-%d %H:%M:%S%z')
                            json_stm['tweet_id'] = lt_count

                            json_stm_ex = json.dumps( json_stm ).replace("'", "`") # Replace sinlge quotes, not supported by cql json insert

                            start_time = time.time()
                            session.execute ( "INSERT INTO tweets JSON '" + json_stm_ex + "'" )
                            end_time = time.time()

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

                print("  Insert Tweet [%i] t-size [%0.2f MB] ...  \r" % (lt_count, total_size) )
                print("  Elapsed_time : ", duration ) 

                iter_durations.append( duration )
                iterations -= 1 
                session.execute ( "DELETE FROM tweets WHERE tweet_id = " + json_stm["tweet_id"] + "" )

            print("%i iterations: %s " % (iterations, str(iter_durations)) )
            avg = sum (iter_durations) / float( max(len(iter_durations), 1) )
            with open ("results/iterations.json","a+") as bulk_f:
                json.dump({"dataset":dataset, "insertOneIndexed": iter_durations,"average":avg}, bulk_f)
                bulk_f.write("\n")

    except Exception as except_inst:
        print("\nDataset Insert error : " + except_inst.__str__())

    session.execute('DROP INDEX IF EXISTS date_index;')
    session.execute('DROP INDEX IF EXISTS lang_index;')
    session.execute('DROP INDEX IF EXISTS location;')
    session.execute('DROP INDEX IF EXISTS followers_count;')
    session.execute('DROP INDEX IF EXISTS friends_count;')