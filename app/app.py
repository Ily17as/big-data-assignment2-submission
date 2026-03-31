from cassandra.cluster import Cluster

# Connect to the local Cassandra service inside Docker.
cluster = Cluster(["cassandra-server"])
session = cluster.connect()

# Show available keyspaces for a quick sanity check.
rows = session.execute("DESC keyspaces")

for row in rows:
    print(row)

session.shutdown()
cluster.shutdown()