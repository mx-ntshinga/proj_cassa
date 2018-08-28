from __future__ import print_function

import time
import json
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

def indexFind(session, iterations, jsn_dataset):
    print("\n===============================\nIndexFind:  \n  Query with Index (created_at) on tweets table/column-family\n")

    find_withIndex = "SELECT tweet_id, created_at, user.screen_name, lang, text From tweets_db.tweets WHERE lang = 'en' and created_at < toTimestamp(now()) ALLOW FILTERING;"  # LIMIT 1000000000"
    print("Query: "+find_withIndex )
    
    try:        
        date_index = "CREATE INDEX IF NOT EXISTS date_index ON tweets (created_at);"
        lang_index = "CREATE INDEX IF NOT EXISTS lang_index ON tweets (lang);"
        followers_index = "CREATE INDEX IF NOT EXISTS followers_count ON tweets (ENTRIES(user_followers_count));"
        location_index = "CREATE INDEX IF NOT EXISTS location ON tweets (ENTRIES(user_location));"
        friends_index = "CREATE INDEX IF NOT EXISTS friends_count ON tweets (ENTRIES(user_friends_count));"

        # Statement.ConsistencyLevel = ConsistencyLevel.ALL  # more than one response ) to ensure no data is missing from querying replicas

        # session.execute(date_index) 
        session.execute(lang_index)
        session.execute(location_index)
        session.execute(followers_index)
        session.execute(friends_index)

        iter_durations = []
        while iterations > 0:
            session.execute("ALTER TABLE tweets WITH GC_GRACE_SECONDS = 0;")  # To clear tombstone buffer so that the query can be able to get results without limitations.

            rows = 0
            duration = 0.0
            print("\nIteration: ",iterations)
            try:
                # lang = en and friends_count > 100 and followers > 100 
                start = time.time()
                rows_returned = session.execute(find_withIndex)
                duration = time.time() - start
                rows = len(rows_returned.current_rows)

            except Exception as error:
                print("\nSELECT stm error : " + error.__str__())

            print ("  Rows returned: ", rows)
            print ("  Elapsed time : ", duration)
            iter_durations.append( duration )
            iterations -= 1

        avg = sum(iter_durations) / float( max(len(iter_durations), 1) )
        with open("results/iterations.json","a+") as bulk_f:
            json.dump({"dataset":jsn_dataset, "findByIndex": iter_durations,"average":avg}, bulk_f)
            bulk_f.write("\n")

        print("%i iterations: %s " % (iterations, str(iter_durations)) )

    except Exception as error:
        print("\nFind by Index error : " + error.__str__())

    try:
        session.execute("DROP INDEX IF EXISTS date_index;")
        session.execute("DROP INDEX IF EXISTS location;")
        session.execute("DROP INDEX IF EXISTS followers_count;")
        session.execute("DROP INDEX IF EXISTS friends_count;")
        session.execute("DROP INDEX IF EXISTS lang_index;")
    except Exception as error:
        print("\nDROP INDEX stm error : " + error.__str__())

    # print( "  %-22s %-34s %-20s %-5s %-5s" % ("tweet_id", "created_at", "user.screen_name", "lang", "text") )
    # for row in rows_returned: print( "  %-22s %-34s %-20s %-5s %-5s" % (row.id, row.created_at, row.user_screen_name, row.lang, row.text.replace('\n','') ) )