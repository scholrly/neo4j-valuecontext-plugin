Neo4j ValueContext Plugin
=========================

Numeric index values in Neo4j can be manipulated trivially using ValueContext_,
but this functionality is not currently exposed to the REST API.
Neo4j ValueContext Plugin extends the `Neo4j REST service`_ to permit indexing of numeric values.

.. _ValueContext: http://api.neo4j.org/current/org/neo4j/index/lucene/ValueContext.html

.. _Neo4j REST service: http://components.neo4j.org/neo4j-server/milestone/rest.html

Example Usage
-------------

Create a "time" index::

 curl -X DELETE http://localhost:7474/db/data/index/node/time
 curl -X POST -H Accept:application/json -HContent-Type:application/json -d \
   '{"name":"time", "config":{"type":"exact","provider":"lucene"}}' \
   http://localhost:7474/db/data/index/node

Give the reference node a timestamp value of 25::

 curl -H Accept:application/json http://localhost:7474/db/data/ext/ValueContextPlugin/node/0/post_long \
   -H "Content-Type: application/json" -d '{"index": "time", "key":"timestamp", "value":25}'

Query the time index for nodes with timestamps between 1 and 8.
The result is empty because 25 ∉ [1, 8]::

 curl -H Accept:application/json http://localhost:7474/db/data/ext/ValueContextPlugin/graphdb/get_long_range_node \
   -H "Content-Type: application/json" -d '{"index": "time", "key":"timestamp", "min":1, "max":8}'

Query the time index for nodes with timestamps between 8 and 30.
The result contains the ref node because 25 ∈ [8, 30]::

 curl -H Accept:application/json http://localhost:7474/db/data/ext/ValueContextPlugin/graphdb/get_long_range_node \
   -H "Content-Type: application/json" -d '{"index": "time", "key":"timestamp", "min":8, "max":30}'

