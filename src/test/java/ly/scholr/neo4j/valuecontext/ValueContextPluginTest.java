package ly.scholr.neo4j.valuecontext;

import junit.framework.Assert;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import java.util.HashMap;
import java.util.Map;

public class ValueContextPluginTest extends AbstractNeo4jTestCase {

  static final String INDEX_NAME = "time";
  static final String KEY_NAME = "timestamp";

  ValueContextPlugin plugin;
  Index<Node> index;
  Node node;

  @Override
  public void setUpTest() {
    super.setUpTest();

    plugin = new ValueContextPlugin();
    node = getGraphDb().getReferenceNode();

    getGraphDb().index().forNodes(INDEX_NAME).delete();
    newTransaction();

    Map<String, String> config = new HashMap<String, String>();
    config.put("provider", "lucene");
    config.put("type", "exact");
    index = getGraphDb().index().forNodes(INDEX_NAME, config);
    newTransaction();

    plugin.postLong(node, INDEX_NAME, KEY_NAME, 25l);
    newTransaction();
  }

  @Test public void testOut1() { assertInRange(false, 1l, 8l); }
  @Test public void testOut2() { assertInRange(false, 8l, 24l); }
  @Test public void testOut3() { assertInRange(false, 26l, 30l); }
  @Test public void testIn1() { assertInRange(true, 8l, 30l); }
  @Test public void testIn2() { assertInRange(true, 8l, 25l); }
  @Test public void testIn3() { assertInRange(true, 25l, 30l); }

  void assertInRange(boolean inRange, long lowerBound, long upperBound) {
    assertSize(inRange ? 1 : 0,
      plugin.getLongRangeNode(getGraphDb(), INDEX_NAME, KEY_NAME, lowerBound, upperBound));
  }

  void assertSize(int expectedSize, Iterable iterable) {
    Assert.assertEquals(expectedSize, sizeof(iterable));
  }

  int sizeof(final Iterable<?> iterable) {
    int i = 0;
    for (Object o : iterable) ++i;
    return i;
  }

}
