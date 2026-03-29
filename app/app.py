from cassandra.cluster import Cluster

cluster = Cluster(["cassandra-server"])
session = cluster.connect()
rows = session.execute("DESC keyspaces")

for row in rows:
    print(row)
    
session.shutdown()
cluster.shutdown()
