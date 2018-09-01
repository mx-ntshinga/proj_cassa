from __future__ import print_function

import time
import json
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

def indexFind(session, iterations, jsn_dataset):
    print("\n===============================\nIndexFind:  \n  Query with Index (lang, location, friends_count, followers_count) on tweets table/column-family\n")

    # tweet_id, created_at, user.screen_name, lang, text From tweets
    # print("Query: "+find_withIndex )

    try:        
        # lang_index = "CREATE INDEX IF NOT EXISTS lang_index ON tweets (lang);"
        date_index = "CREATE INDEX IF NOT EXISTS date_index ON tweets (created_at);"
        location_index = "CREATE INDEX IF NOT EXISTS location ON tweets (user_followers_count);"
        followers_index = "CREATE INDEX IF NOT EXISTS followers_count ON tweets (user_followers_count);"
        friends_index = "CREATE INDEX IF NOT EXISTS friends_count ON tweets (user_friends_count);"

        # Statement.ConsistencyLevel = ConsistencyLevel.ALL  # more than one response ) to ensure no data is missing from querying replicas

        # session.execute(lang_index)
        session.execute(date_index) 
        session.execute(location_index)
        session.execute(followers_index)
        session.execute(friends_index)

        find_withIndex1 = "SELECT count(tweet_id) from tweets WHERE lang = 'en' and token(tweet_id) >= -9223372036854775808 ALLOW FILTERING;"
        find_withIndex2 = "SELECT count(user_location) from tweets WHERE user_location= 'London' and token(tweet_id) >= -9223372036854775808 ALLOW FILTERING;"
        find_withIndex3 = "SELECT count(user_followers_count) from tweets WHERE user_followers_count >= 1000 and token(tweet_id) >= -9223372036854775808 ALLOW FILTERING;"
        find_withIndex4 = "SELECT count(user_friends_count) from tweets WHERE user_friends_count >= 1000 and token(tweet_id) >= -9223372036854775808 ALLOW FILTERING;"

        # find_withIndex = "SELECT count(tweet_id) WHERE lang = 'en' AND user.location = 'London' \
        #               AND user.followers_count>=1000 AND user.friends_count>=1000 and created_at < toTimestamp(now()) ALLOW FILTERING;"  # LIMIT 1000000000"

        iter_durations = []
        while iterations > 0:
            # session.execute("ALTER TABLE tweets WITH GC_GRACE_SECONDS = 0;")  # To clear tombstone buffer for single node - so that the query can be able to get results without exceeding limitations.

            rows = 0
            duration = 0.0
            print("\nIteration: ",iterations)
            try:
                # lang = en and friends_count > 100 and followers > 100 
                start1 = time.time()
                rows_returned1 = session.execute(find_withIndex1)
                duration = time.time() - start1

                start2 = time.time()
                rows_returned2 = session.execute(find_withIndex2)
                duration = time.time() - start2

                start3 = time.time()
                rows_returned3 = session.execute(find_withIndex3)
                duration = time.time() - start3

                start4 = time.time()
                rows_returned4 = session.execute(find_withIndex4)
                duration += time.time() - start4

                rows = len(rows_returned1.current_rows)+len(rows_returned2.current_rows)+len(rows_returned3.current_rows)+len(rows_returned4.current_rows)

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
        print("\nFind by Index error : \n" + error.__str__())

    try:
        # session.execute("DROP INDEX IF EXISTS lang_index;")
        session.execute("DROP INDEX IF EXISTS date_index;")
        session.execute("DROP INDEX IF EXISTS location_index;")
        session.execute("DROP INDEX IF EXISTS followers_count;")
        session.execute("DROP INDEX IF EXISTS friends_count;")
    except Exception as error:
        print("\nDROP INDEX stm error : " + error.__str__())

    # print( "  %-22s %-34s %-20s %-5s %-5s" % ("tweet_id", "created_at", "user.screen_name", "lang", "text") )
    # for row in rows_returned: print( "  %-22s %-34s %-20s %-5s %-5s" % (row.id, row.created_at, row.user_screen_name, row.lang, row.text.replace('\n','') ) )