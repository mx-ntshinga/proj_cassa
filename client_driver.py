from __future__ import print_function
# Cassandra Python Driver

#====================================
# column family / data model setup
    # key
    # columns & data types
    # indexes (primary, secondary)
    # consistency

# Variety of metrics for cache size, hit rate, client request latency, thread pool status, 
# per column family statistics, and other operational measurements.

#====================================
# API
#====================================
# Connect to keyspace and query:
# Insert / scan / Updates

import json
import time
import datetime 
import os
import sys
from sys import getsizeof

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from datetime import datetime

from insertOne import * 
from inserts_bulk import * 
from scanning import * 
from model_simulation import * 
from indexFind import * 
from insertOneIndexed import * 

# Start connection to a Cassandra instance on a machine (local 127.0.0.1 for this application)
def keyscapeSetup(tweets_db):
    
    print("Config: Keyspace: \n=================")
    # print("Cluster Details: \n================")
    
    # print ("Replication factor, Node-1 IP, Port number")
    # factor = sys.argv [0] 
    # location = sys.argv [1]
    # portNumber = sys.argv [2]

    """
    Node
    ----
        OS      : Ubuntu 17.10
        memory  :
        CPU     : 
        DIsk    : 
    """

    # Default
    factor = "1"
    # location = ['127.0.0.1']
    location = ['137.158.59.83']
    # location = ['137.158.59.88','137.158.59.83','137.158.59.86']
    portNumber = 9042
    nodes = factor

    if factor:
        print("\nDatacenter cluster:")
        print(" * IP = %s  \n * PortNumber = %i  \n * Nodes = %i " % (str(location), portNumber, eval(nodes)) )
        print()
        print("Keyspace (" + tweets_db + "):")
        print(" * Strategy = SimpleStrategy\n * Replication Factor = %i" % (eval(factor)) )
        print(" * Partitioner : MurMur3Partitioner" )

        # print( "Replication Factor = '"+factor+"',  location = '"+str(location)+"',  portNumber = '"+str(portNumber)+"'")
        try:
            cluster = Cluster( location, port=portNumber)  # (['192.168.1.1', '192.168.1.2'])
            session = cluster.connect(tweets_db)
            # ser_lookup_stmt.consistency_level = ConsistencyLevel.
            print ("\n... Connected to cluster!\n")
            # options = optionsKeyspace(session, tweets_db, strategy="SimpleStrategy", replic=factor )
            return [session, cluster]

        except Exception as exp:
            print("\nkeyscapeSetup error: " + exp.__str__() )
            exit()
    else:
        print("input invalid!")
        cluster.shutdown()
        return None

def optionsKeyspace(session, tweets_db, strategy, replic):

    if session:
        print("Options :")
        print("=============================")
        print(" 1. Create table/column-family (tweets)  \n 2. Drop table/column-family (tweets) \n 3. Truncate table/column-family (tweets) \n 4. No options (Press enter)!")
        config_keysp = "start"

        while config_keysp in ["start","1","2","3", "4"]:
            config_keysp = raw_input ("\nSelect option: ")

            if config_keysp =="1":
                try:
                    with open("schema/udd_types.cql", mode="r") as cqlFile:
                        udd_types = cqlFile.read().replace('\n', '')
                        print(udd_types)
                        session.execute(udd_types)
                    with open("schema/table.cql", mode="r") as cqlFile:
                        table_schema = cqlFile.read().rstrip("\n")
                        # print(table_schema)
                        session.execute(table_schema)
                except Exception as except_inst:
                    print("\nCreate table schema error : " + except_inst.__str__())

            elif config_keysp =="2":
                try:
                    session.execute("DROP TABLE tweets;")
                    print("TABLE removed!")
                except Exception as except_inst:
                    print("\nCreate table schema error : " + except_inst.__str__())
            elif config_keysp =="3":
                try:
                    session.execute("TRUNCATE TABLE tweets;")
                    print("TABLE data emptied!")
                except Exception as except_inst:
                    print("\nCreate table schema error : " + except_inst.__str__())
            else:
                break
    else:
        print("session not setup!")

def main():
    print("\nApache Cassandra - Client Driver API\n")
    
    cassandra_db = "tweets_db"
    keyscape = keyscapeSetup( cassandra_db )
    session = keyscape[0]
    cluster = keyscape[1]

    tweets_limit = 1000000


    print("\nOptions: Database performance measurements:\n===========================================")
    print(" 1: Automatic Testing of (1. BulkLoad Insert, 2. ScanAll, 3. IndexFind, 4. Insert One, 5. InsertOne Indexed) \
         \n 2: Run 'Write + Scan + Update' simultaneously (Simulate live busy DB) \n q: Quit")

    option = ""
    try:
        #jsn_dataset = "Eurovision9.json"
        iterations = 20
        if len(sys.argv)>1:
            jsn_dataset = sys.argv[1] #"dataset/Eurovision3.json"
        else:
            print()
            print("Missing dataset filename argument: e.g. python client_driver.py 'dataset'")
            print("... Defaulting to options 1 or 7.]")
            option = "1"
            # exit()

        if not option.upper() == "Q":
            option =  raw_input("\nSelect option : ")
            # datasets = ["dataset/e3-5MB.json", "dataset/e3-50MB.json", "dataset/e3-100MB.json", "dataset/e3-500MB.json", "dataset/e10-1GB.json"]

            if len(sys.argv)>1:
                # BULK INSERTS
                if option == "4":
                    bulk_inserts (dataset = jsn_dataset, session=session, cassandra_db=cassandra_db, iterations=20)

            # AUTOMATE TESTING
            if option == "1":  # Automated iteration tests
                # datasets = ["dataset/e10-1GB.json"]
                # datasets = ["dataset/e3-100MB.json", "dataset/e3-500MB.json", "dataset/e10-1GB.json"]
                datasets = ["dataset/e3-5MB.json", "dataset/e3-50MB.json", "dataset/e3-100MB.json", "dataset/e3-500MB.json", "dataset/e10-1GB.json"]
                tweets_limit = 1000000

                for jsn_dataset in datasets:
                    bulk_inserts (dataset = jsn_dataset, session=session, cassandra_db=cassandra_db, iterations=20)
                    scanAll (session, iterations=20, jsn_dataset=jsn_dataset)
                    indexFind (session, iterations=20, jsn_dataset=jsn_dataset)
                insertOne (dataset = "dataset/single.json", session=session, cassandra_db=cassandra_db,  iterations=20)                
                insertOneIndexed (dataset = "dataset/single.json", session=session, cassandra_db=cassandra_db,  iterations=20)
            # RUN ALL SIMULATANEOUSLY
            elif option == "2":
                # print("All four (write, update, delete) CRUD operations running synchronized.")
                print("Multiple 'n' Threads for all CRUD operations running concurrently.***")
                dur = run_simulation()
            # OTHER OPTIONS
            elif option.upper() == "Q":
                pass
            else:
                print("Input '" + option + "'' Not accepted")
                exit()

    except KeyboardInterrupt as keyb:
        print("\n\nOperation cancelled!") 

    except Exception as error:
        print("\n" + error.__str__())

    # cluster.shutdown()

if __name__ == "__main__":
    main()
