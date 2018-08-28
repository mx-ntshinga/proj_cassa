#!/usr/bin/python
from __future__ import print_function

import json
import time
import datetime 
import os
import sys
from sys import getsizeof

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from datetime import datetime  

import threading
import time
import random

exitFlag = 0

# Threads:
#   insert x items from file x
#   update x items where timestamp >= "" or where field >= "" from file x
#   delete x items where field >= ""
#   search x items 

# LOADS FORMAT 
# inserts load = ("filename", 10000)

# updates load:
# 1. update row where where col=""
# 2. update row where where col=""
# 3. update row where where col=""
# ...
# deletes load:
# 1. delete row where where col=""
# 2. delete row where where col=""
# 3. delete row where where col=""
# ...
# searchs load = {"term1":"col", "term2":"col"} 
# 1. delete row where where col=""
# 2. delete row where where col=""
# 3. delete row where where col=""
# ...
# scan load = {"term1":"col", "term2":"col"}
# 1. scan table for condition col=""
# 2. scan table for condition col=""
# 3. scan table for condition col=""
# ...

threadLock = threading.Lock()

class Simulate (threading.Thread):
    def __init__(self, threadID, name, load):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.load = load
        self.duration = 0.0
   
    def run(self):  
        print ("Starting " + self.name)
        print ("")
        # Get lock to synchronize threads
        self.do_work(self.name, self.load)
        # print "Done " + self.name

    def do_work(self, name, load):
        # while load > 0:
        print ("Run %s @%s"%(name, time.time()) )
        if name == "inserts": # name = session, name, load, filename
            insert(limit=10, dataset="dataset/Eurovision3.json")
        elif name == "updates": # name = session, name, load, filename
            update(limit=10, update_records="livedb_dataset/updates.json")
        elif name == "searches": # name = session, name, load, filename
            search(limit=10, searching_records="livedb_dataset/searches.json")

        # while load > 0:
        #     # print("session execute ( %s )"%name)
        #     # threadLock.acquire() 
        #     start = time.time()
        #     # session.execute ( name )
        #     end = time.time()
        #     self.duration += (end-start)
        #     print("" + name)
        #     load -= 1 
        #     # threadLock.release()
        #     time.sleep( 0.2 )
 
def insert(limit, dataset):
    session_i = connect ("tweets_db", "insert")
    session = session_i[0]
    avg_latency = 0.0

    jsonDataset = None
    try: # file open level
        with open(dataset, mode="r") as jsonDataset_jsn:
            jsonDataset = jsonDataset_jsn.readlines()
    except Exception as except_inst:
        print("\nDataset read error : " + except_inst.__str__())
    
    try:
        lt_count = 1
            # Iterate over JSON items (i.e. Tweets in json file) 
        for a_tweet in jsonDataset:
            start_time = time.time()
            if lt_count < limit and not ("limit" in a_tweet and "track" in a_tweet):   # # Flag out columns with no primary key 
                print("Insert!")
                # json tweet line  
                try:
                    json_stm = json.loads(a_tweet) # item in json dataset
                    # total_size  += (float(sys.getsizeof( json_stm )) / 1000000.0)

                    # REFORMATING while DECODING JSON values for cassandra's accepted data types
                    try:
                        if 'retweeted_status' in json_stm and 'quoted_status' in json_stm['retweeted_status']:
                            json_stm["retweeted_status"]["quoted_status"] = str(json_stm['retweeted_status']['quoted_status'])

                            json_stm_ex = json.dumps( json_stm ).replace("'", "`") # Replace sinlge quotes, not supported by cql json insert
                            session.execute ( "INSERT INTO tweets JSON '" + json_stm_ex + "'" )
                    except Exception as exp_t:
                        stm_error = "\nStatement execute error : " + exp_t.__str__() + "\n " + json.dumps(json_stm, indent=4) 
                        # Checking for new schema change  [ ADD New Column ]
                        print(stm_error)
                        break

                    # record event time
                    end_time = time.time()
                    elapsed_time = end_time - start_time

                    # Record METRICS [ LATENCY,  THROUGHPUT, AVERAGES ] 
                    avg_latency = elapsed_time / lt_count  
                except KeyboardInterrupt as keyb:
                    print("\n\nOperation cancelled!")
                    break   

                except Exception as error:
                    print("\n" + error.__str__())
            lt_count += 1
    except Exception as except_inst:
        print("\nDataset read error : " + except_inst.__str__())

    session_i[1].shutdown()
    return avg_latency

def update(limit, update_records):
    session_u = connect ("tweets_db", "update")
    session = session_u[0]
    
    # update  where len(text) is greater than number '50'
    # update [delete] where lang is not en, xh, etc.
    duration = 0.0

    try:
        with open(update_records, mode="r") as update_records:

            update_records_json = json.load(update_records)

            for update in update_records_json["updates"]:
                method = json.dumps(update [0])
                column =  json.dumps(update [2])
                value =  json.dumps(update [3])
                statement = None
                # id, created_at, user.screen_name, lang, text 
                if method=="UPDATE":
                    statement = "UPDATE tweets_db.tweets SET %s = %s \
                                 WHERE token(id)>=-9223372036854775808 AND %s = '%s' ;" % (column, value, column, "null")
                else:
                    statement = "DELETE %s from tweets_db.tweets  \
                                 WHERE id=92233734470474 AND %s = '%s' ;" % (column, column, value)
                if statement:
                    start = time.time()
                    session.execute ( statement )
                    duration += time.time() - start
                    print( column + " updated!")
    
        # print("Update: Elapse_time (%i s) " % duration)
    
    except Exception as exc:
        print( exc.__str__() )

    session_u[1].shutdown()
    return duration

def search(limit, searching_records):
    session_s = connect ("tweets_db", "search")
    session = session_s[0]

    results = []
    duration = 0.0
    try:
        with open(searching_records, mode="r") as searching_records:

            searching_records_json = json.load(searching_records)

            for search in searching_records_json["searches"]:
                column = json.dumps(search [0])
                term = json.dumps(search [1])

                statement = "SELECT id, created_at, user.screen_name, lang, text From tweets_db.tweets \
                             WHERE %s = '%s' LIMIT 1 ALLOW FILTERING;" % (column, term)
                start = time.time()
                result_set = session.execute ( statement )
                duration += time.time() - start
                # results.append( result_set )

                if len(result_set.curren_rows)>0:
                    print( "Term '" + term + "' found in column '"+ column + "'")

    except Exception as exc:
        print( exc.__str__() )
    
    session_s[1].shutdown()
    return duration
    
    # print( "  %-22s %-34s %-20s %-5s %-5s" % ("id", "created_at", "user.screen_name", "lang", "text") )
    # for row in result_set :
    #     print( "  %-22s %-34s %-20s %-5s %-5s" % (row.id, row.created_at, row.user_screen_name, row.lang, row.text.replace('\n','') ) )

    session_s[1].shutdown()

def connect(tweets_db, prog):
    # Default
    factor = "2"
    location = ['127.0.0.1']
    # location = ['137.158.59.91']
    portNumber = 9042
    nodes = factor

    if factor:
        # print ("\nDatacenter:")
        # print (" * IP = %s  \n * portNumber = %i  \n * Strategy = SimpleStrategy  \n * Nodes = %i  \n * Replication Factor = %i" % (str(location), portNumber, eval(factor), eval(nodes)) )
        # print( "Replication Factor = '"+factor+"',  location = '"+str(location)+"',  portNumber = '"+str(portNumber)+"'")
        try:
            cluster = Cluster( location, port=portNumber)  # (['192.168.1.1', '192.168.1.2'])
            session = cluster.connect(tweets_db)
            # ser_lookup_stmt.consistency_level = ConsistencyLevel.
            print ("%s Connected! \n" % prog)
            return [session, cluster]

        except Exception as exp:
            print("\nkeyscapeSetup error: " + exp.__str__() )
    else:
        print("input invalid!")
        cluster.shutdown()
        return None

def run_simulation():
    session = "Cassandra session"
    # Create threads
    thread1 = Simulate(1, "inserts", 10)
    thread2 = Simulate(2, "updates", 10)
    thread3 = Simulate(3, "searches",10) 

    # Start threads
    thread1.start()
    thread2.start()
    thread3.start() 

    # Wait for threads to finish
    thread1.join()
    thread2.join()
    thread3.join()  

    print("\nExiting Main Thread")
    print("inserts (load=10)  Elapsed : %s seconds" % thread1.duration)
    print("updates (load=10)  Elapsed : %s seconds" % thread2.duration)
    print("searches (load=10)  Elapsed : %s seconds" % thread3.duration) 

if __name__ == "__main__":
    main()
 