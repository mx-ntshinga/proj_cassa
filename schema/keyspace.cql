/* Cassandra CQL language */
CREATE KEYSPACE IF NOT EXISTS Tweets_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
ALTER KEYSPACE "tweets_db" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };