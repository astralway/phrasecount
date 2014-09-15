package phrasecount;

import static phrasecount.Constants.DOC_CONTENT_COL;
import static phrasecount.Constants.DOC_REF_COUNT_COL;
import static phrasecount.Constants.STAT_DOC_COUNT_COL;
import static phrasecount.Constants.STAT_SUM_COL;
import static phrasecount.Constants.TYPEL;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.client.MiniFluo;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Column;
import io.fluo.api.types.TypedSnapshot;
import io.fluo.api.types.TypedSnapshotBase.Value;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

//TODO make this an integration test

public class PhraseCounterTest {
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  private static FluoConfiguration props;
  private static MiniFluo miniFluo;
  private static final PasswordToken password = new PasswordToken("secret");
  private static AtomicInteger tableCounter = new AtomicInteger(1);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), new String(password.getPassword()));
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }

  @Before
  public void setUpFluo() throws Exception {
    // TODO add helper code to make this shorter
    props = new FluoConfiguration();
    props.setAccumuloInstance(cluster.getInstanceName());
    props.setAccumuloUser("root");
    props.setAccumuloPassword("secret");
    props.setZookeeperRoot("/fluo");
    props.setZookeepers(cluster.getZooKeepers());
    props.setClearZookeeper(true);
    props.setAccumuloTable("data" + tableCounter.getAndIncrement());
    props.setWorkerThreads(5);
    props.setObservers(Arrays.asList(new ObserverConfiguration(PhraseCounter.class.getName()), new ObserverConfiguration(HCCounter.class.getName())));

    FluoFactory.newAdmin(props).initialize();

    miniFluo = FluoFactory.newMiniFluo(props);
    miniFluo.start();
  }

  @After
  public void tearDownFluo() throws Exception {
    miniFluo.stop();
  }
  
  static class PhraseInfo {
    public PhraseInfo() {}

    public PhraseInfo(int s, int n) {
      this.sum = s;
      this.numDocs = n;
    }
    int sum;
    int numDocs;

    public boolean equals(Object o) {
      if (o instanceof PhraseInfo) {
        PhraseInfo opi = (PhraseInfo) o;
        return sum == opi.sum && numDocs == opi.numDocs;
      }

      return false;
    }

    public String toString() {
      return numDocs + " " + sum;
    }
  }

  private PhraseInfo getPhraseInfo(FluoClient fluoClient, String phrase) throws Exception {

    TypedSnapshot tsnap = TYPEL.wrap(fluoClient.newSnapshot());

    Map<Column,Value> map = tsnap.get().row("phrase:" + phrase).columns(STAT_SUM_COL, STAT_DOC_COUNT_COL);

    if (map.size() == 0)
      return null;

    PhraseInfo pi = new PhraseInfo();
    pi.sum = map.get(STAT_SUM_COL).toInteger();
    pi.numDocs = map.get(STAT_DOC_COUNT_COL).toInteger();

    return pi;
  }
  
  private void loadDocument(LoaderExecutor le, String uri, String content) {
    Document doc = new Document(uri, content);
    le.execute(new DocumentLoader(doc));
    miniFluo.waitForObservers();
  }

  @Test
  public void test1() throws Exception {

    FluoConfiguration lep = new FluoConfiguration(props);
    lep.setLoaderThreads(0);
    lep.setLoaderQueueSize(0);
    
    FluoClient fluoClient = FluoFactory.newClient(lep);

    LoaderExecutor le = fluoClient.newLoaderExecutor();

    loadDocument(le, "/foo1", "This is only a test.  Do not panic. This is only a test.");

    Assert.assertEquals(new PhraseInfo(2, 1), getPhraseInfo(fluoClient, "is only a test"));
    Assert.assertEquals(new PhraseInfo(1, 1), getPhraseInfo(fluoClient, "test do not panic"));

    // add new document w/ different content and overlapping phrase.. should change some counts
    loadDocument(le, "/foo2", "This is only a test");

    Assert.assertEquals(new PhraseInfo(3, 2), getPhraseInfo(fluoClient, "is only a test"));
    Assert.assertEquals(new PhraseInfo(1, 1), getPhraseInfo(fluoClient, "test do not panic"));

    // add new document w/ same content, should not change any counts
    loadDocument(le, "/foo3", "This is only a test");

    Assert.assertEquals(new PhraseInfo(3, 2), getPhraseInfo(fluoClient, "is only a test"));
    Assert.assertEquals(new PhraseInfo(1, 1), getPhraseInfo(fluoClient, "test do not panic"));

    // change the content of /foo1, should change counts
    loadDocument(le, "/foo1", "The test is over, for now.");

    Assert.assertEquals(new PhraseInfo(1, 1), getPhraseInfo(fluoClient, "the test is over"));
    Assert.assertEquals(new PhraseInfo(1, 1), getPhraseInfo(fluoClient, "is only a test"));
    Assert.assertNull(getPhraseInfo(fluoClient, "test do not panic"));

    // change content of foo2, should not change anything
    loadDocument(le, "/foo2", "The test is over, for now.");

    Assert.assertEquals(new PhraseInfo(1, 1), getPhraseInfo(fluoClient, "the test is over"));
    Assert.assertEquals(new PhraseInfo(1, 1), getPhraseInfo(fluoClient, "is only a test"));
    Assert.assertNull(getPhraseInfo(fluoClient, "test do not panic"));

    String oldHash = new Document("/foo3", "This is only a test").getHash();
    TypedSnapshot tsnap = TYPEL.wrap(fluoClient.newSnapshot());
    Assert.assertNotNull(tsnap.get().row("doc:" + oldHash).col(DOC_CONTENT_COL).toString());
    Assert.assertEquals(1, tsnap.get().row("doc:" + oldHash).col(DOC_REF_COUNT_COL).toInteger(0));

    // dereference document that foo3 was referencing
    loadDocument(le, "/foo3", "The test is over, for now.");

    Assert.assertEquals(new PhraseInfo(1, 1), getPhraseInfo(fluoClient, "the test is over"));
    Assert.assertNull(getPhraseInfo(fluoClient, "is only a test"));
    Assert.assertNull(getPhraseInfo(fluoClient, "test do not panic"));

    tsnap = TYPEL.wrap(fluoClient.newSnapshot());
    Assert.assertNull(tsnap.get().row("doc:" + oldHash).col(DOC_CONTENT_COL).toString());
    Assert.assertNull(tsnap.get().row("doc:" + oldHash).col(DOC_REF_COUNT_COL).toInteger());

    le.close();
    fluoClient.close();

  }

  @Test
  public void testHighCardinality() throws Exception {
    FluoConfiguration lep = new FluoConfiguration(props);
    lep.setLoaderThreads(0);
    lep.setLoaderQueueSize(0);

    FluoClient fluoClient = FluoFactory.newClient(lep);

    LoaderExecutor le = fluoClient.newLoaderExecutor();

    Random rand = new Random();

    loadDocsWithRandomWords(le, rand, "This is only a test", 0, 100);

    Assert.assertEquals(new PhraseInfo(100, 100), getPhraseInfo(fluoClient, "this is only a"));
    Assert.assertEquals(new PhraseInfo(100, 100), getPhraseInfo(fluoClient, "is only a test"));

    loadDocsWithRandomWords(le, rand, "This is not a test", 0, 2);

    Assert.assertEquals(new PhraseInfo(2, 2), getPhraseInfo(fluoClient, "this is not a"));
    Assert.assertEquals(new PhraseInfo(2, 2), getPhraseInfo(fluoClient, "is not a test"));
    Assert.assertEquals(new PhraseInfo(98, 98), getPhraseInfo(fluoClient, "this is only a"));
    Assert.assertEquals(new PhraseInfo(98, 98), getPhraseInfo(fluoClient, "is only a test"));

    loadDocsWithRandomWords(le, rand, "This is not a test", 2, 100);

    Assert.assertEquals(new PhraseInfo(100, 100), getPhraseInfo(fluoClient, "this is not a"));
    Assert.assertEquals(new PhraseInfo(100, 100), getPhraseInfo(fluoClient, "is not a test"));
    Assert.assertEquals(new PhraseInfo(0, 0), getPhraseInfo(fluoClient, "this is only a"));
    Assert.assertEquals(new PhraseInfo(0, 0), getPhraseInfo(fluoClient, "is only a test"));

    loadDocsWithRandomWords(le, rand, "This is only a test", 0, 50);

    Assert.assertEquals(new PhraseInfo(50, 50), getPhraseInfo(fluoClient, "this is not a"));
    Assert.assertEquals(new PhraseInfo(50, 50), getPhraseInfo(fluoClient, "is not a test"));
    Assert.assertEquals(new PhraseInfo(50, 50), getPhraseInfo(fluoClient, "this is only a"));
    Assert.assertEquals(new PhraseInfo(50, 50), getPhraseInfo(fluoClient, "is only a test"));

    le.close();
    fluoClient.close();
  }

  void loadDocsWithRandomWords(LoaderExecutor le, Random rand, String phrase, int start, int end) {
    // load many documents that share the same phrase
    for (int i = start; i < end; i++) {
      String uri = "/foo" + i;
      StringBuilder content = new StringBuilder(phrase);
      // add a bunch of random words
      for (int j = 0; j < 20; j++) {
        content.append(' ');
        content.append(Integer.toString(rand.nextInt(10000), 36));
      }

      Document doc = new Document(uri, content.toString());
      le.execute(new DocumentLoader(doc));
    }

    miniFluo.waitForObservers();
  }
}
  
