package ly.scholr.neo4j.valuecontext;

import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.server.plugins.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ValueContextPlugin extends ServerPlugin {

  @SuppressWarnings("unchecked")
  private static <T> Iterable<T> nothing() { return nothing; }

  private static final Iterable nothing = new Iterable() {
    @Override
    public Iterator iterator() {
      return new Iterator() {
        @Override public boolean hasNext() { return false; }
        @Override public Object next() { throw new NoSuchElementException(); }
        @Override public void remove() { throw new UnsupportedOperationException(); }
      };
    }
  };

  private static <T extends PropertyContainer> Iterable<T> postNumeric(
      final T t, final Index<T> index, final String key, final Number value) {

    Transaction tx = t.getGraphDatabase().beginTx();
    try {
      index.add(t, key, ValueContext.numeric(value));
      tx.success();
    } finally {
      tx.finish();
    }
    return nothing();
  }

  @Name("post_int")
  @PluginTarget(Node.class)
  public Iterable<Node> postIntNode(
      @Source Node node,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "value") Integer value) {

    Index<Node> index = node.getGraphDatabase().index().forNodes(indexName);
    return postNumeric(node, index, key, value);
  }

  @Name("post_int")
  @PluginTarget(Relationship.class)
  public Iterable<Relationship> postIntRelationship(
      @Source Relationship relationship,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "value") Integer value) {

    Index<Relationship> index = relationship.getGraphDatabase().index().forRelationships(indexName);
    return postNumeric(relationship, index, key, value);
  }

  @Name("post_long")
  @PluginTarget(Node.class)
  public Iterable<Node> postLongNode(
      @Source Node node,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "value") Long value) {

    Index<Node> index = node.getGraphDatabase().index().forNodes(indexName);
    return postNumeric(node, index, key, value);
  }

  @Name("post_long")
  @PluginTarget(Relationship.class)
  public Iterable<Relationship> postLongRelationship(
      @Source Relationship relationship,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "value") Long value) {

    Index<Relationship> index = relationship.getGraphDatabase().index().forRelationships(indexName);
    return postNumeric(relationship, index, key, value);
  }

  @Name("post_float")
  @PluginTarget(Node.class)
  public Iterable<Node> postFloatNode(
      @Source Node node,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "value") Float value) {

    Index<Node> index = node.getGraphDatabase().index().forNodes(indexName);
    return postNumeric(node, index, key, value);
  }

  @Name("post_float")
  @PluginTarget(Relationship.class)
  public Iterable<Relationship> postFloatRelationship(
      @Source Relationship relationship,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "value") Float value) {

    Index<Relationship> index = relationship.getGraphDatabase().index().forRelationships(indexName);
    return postNumeric(relationship, index, key, value);
  }

  @Name("post_double")
  @PluginTarget(Node.class)
  public Iterable<Node> postDoubleNode(
      @Source Node node,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "value") Double value) {

    Index<Node> index = node.getGraphDatabase().index().forNodes(indexName);
    return postNumeric(node, index, key, value);
  }

  @Name("post_double")
  @PluginTarget(Relationship.class)
  public Iterable<Relationship> postDoubleRelationship(
      @Source Relationship relationship,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "value") Double value) {

    Index<Relationship> index = relationship.getGraphDatabase().index().forRelationships(indexName);
    return postNumeric(relationship, index, key, value);
  }

  private static <T extends PropertyContainer> Iterable<T> getNumericRange(
      final GraphDatabaseService graphDb, final Index<T> index,
      final String key, final Object query, final Sort sort) {

    Iterable<T> hits;
    Transaction tx = graphDb.beginTx();
    try {
      hits = index.query(key, new QueryContext(query).sort(sort));
      tx.success();
    } finally {
      tx.finish();
    }
    return hits;
  }

  private static <T extends PropertyContainer> Iterable<T> getIntRange(
      final GraphDatabaseService graphDb, final Index<T> index, final String key,
      final Integer minNullable, final Integer maxNullable) {

    int min = minNullable == null ? Integer.MIN_VALUE : minNullable;
    int max = maxNullable == null ? Integer.MAX_VALUE : maxNullable;
    Object query = NumericRangeQuery.newIntRange(key, min, max, true, true);
    Sort sort = new Sort(new SortField(key, SortField.INT, false));
    return getNumericRange(graphDb, index, key, query, sort);
  }

  @Name("get_int_range_node")
  @PluginTarget(GraphDatabaseService.class)
  public Iterable<Node> getIntRangeNode(
      @Source GraphDatabaseService graphDb,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "min") Integer min,
      @Parameter(name = "max") Integer max) {

    Index<Node> index = graphDb.index().forNodes(indexName);
    return getIntRange(graphDb, index, key, min, max);
  }

  @Name("get_int_range_relationship")
  @PluginTarget(GraphDatabaseService.class)
  public Iterable<Relationship> getIntRangeRelationship(
      @Source GraphDatabaseService graphDb,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "min") Integer min,
      @Parameter(name = "max") Integer max ) {

    Index<Relationship> index = graphDb.index().forRelationships(indexName);
    return getIntRange(graphDb, index, key, min, max);
  }

  private static <T extends PropertyContainer> Iterable<T> getLongRange(
      final GraphDatabaseService graphDb, final Index<T> index, final String key,
      final Long minNullable, final Long maxNullable) {

    long min = minNullable == null ? Long.MIN_VALUE : minNullable;
    long max = maxNullable == null ? Long.MAX_VALUE : maxNullable;
    Object query = NumericRangeQuery.newLongRange(key, min, max, true, true);
    Sort sort = new Sort(new SortField(key, SortField.LONG, false));
    return getNumericRange(graphDb, index, key, query, sort);
  }

  @Name("get_long_range_node")
  @PluginTarget(GraphDatabaseService.class)
  public Iterable<Node> getLongRangeNode(
      @Source GraphDatabaseService graphDb,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "min") Long min,
      @Parameter(name = "max") Long max) {

    Index<Node> index = graphDb.index().forNodes(indexName);
    return getLongRange(graphDb, index, key, min, max);
  }

  @Name("get_long_range_relationship")
  @PluginTarget(GraphDatabaseService.class)
  public Iterable<Relationship> getLongRangeRelationship(
      @Source GraphDatabaseService graphDb,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "min") Long min,
      @Parameter(name = "max") Long max) {

    Index<Relationship> index = graphDb.index().forRelationships(indexName);
    return getLongRange(graphDb, index, key, min, max);
  }

  private static <T extends PropertyContainer> Iterable<T> getFloatRange(
      final GraphDatabaseService graphDb, final Index<T> index, final String key,
      final Float minNullable, final Float maxNullable) {

    float min = minNullable == null ? Float.MIN_VALUE : minNullable;
    float max = maxNullable == null ? Float.MAX_VALUE : maxNullable;
    Object query = NumericRangeQuery.newFloatRange(key, min, max, true, true);
    Sort sort = new Sort(new SortField(key, SortField.FLOAT, false));
    return getNumericRange(graphDb, index, key, query, sort);
  }

  @Name("get_float_range_node")
  @PluginTarget(GraphDatabaseService.class)
  public Iterable<Node> getFloatRangeNode(
      @Source GraphDatabaseService graphDb,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "min") Float min,
      @Parameter(name = "max") Float max) {

    Index<Node> index = graphDb.index().forNodes(indexName);
    return getFloatRange(graphDb, index, key, min, max);
  }

  @Name("get_float_range_relationship")
  @PluginTarget(GraphDatabaseService.class)
  public Iterable<Relationship> getFloatRangeRelationship(
      @Source GraphDatabaseService graphDb,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "min") Float min,
      @Parameter(name = "max") Float max) {

    Index<Relationship> index = graphDb.index().forRelationships(indexName);
    return getFloatRange(graphDb, index, key, min, max);
  }

  private static <T extends PropertyContainer> Iterable<T> getDoubleRange(
      final GraphDatabaseService graphDb, final Index<T> index, final String key,
      final Double minNullable, final Double maxNullable) {

    double min = minNullable == null ? Double.MIN_VALUE : minNullable;
    double max = maxNullable == null ? Double.MAX_VALUE : maxNullable;
    Object query = NumericRangeQuery.newDoubleRange(key, min, max, true, true);
    Sort sort = new Sort(new SortField(key, SortField.DOUBLE, false));
    return getNumericRange(graphDb, index, key, query, sort);
  }

  @Name("get_double_range_node")
  @PluginTarget(GraphDatabaseService.class)
  public Iterable<Node> getDoubleRangeNode(
      @Source GraphDatabaseService graphDb,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "min") Double min,
      @Parameter(name = "max") Double max) {

    Index<Node> index = graphDb.index().forNodes(indexName);
    return getDoubleRange(graphDb, index, key, min, max);
  }

  @Name("get_double_range_relationship")
  @PluginTarget(GraphDatabaseService.class)
  public Iterable<Relationship> getDoubleRangeRelationship(
      @Source GraphDatabaseService graphDb,
      @Parameter(name = "index") String indexName,
      @Parameter(name = "key") String key,
      @Parameter(name = "min") Double min,
      @Parameter(name = "max") Double max) {

    Index<Relationship> index = graphDb.index().forRelationships(indexName);
    return getDoubleRange(graphDb, index, key, min, max);
  }

}
