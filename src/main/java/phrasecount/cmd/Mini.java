package phrasecount.cmd;

import static phrasecount.Constants.EXPORT_CHECK_COL;
import static phrasecount.Constants.INDEX_CHECK_COL;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;

import phrasecount.PhraseCounter;
import phrasecount.PhraseExporter;
import accismus.api.Admin;
import accismus.api.Column;
import accismus.api.config.AccismusProperties;
import accismus.api.config.InitializationProperties;
import accismus.api.test.MiniAccismus;

public class Mini {
  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("Usage : " + Mini.class.getName() + " <MAC dir> <output props file>");
      System.exit(-1);
    }

    MiniAccumuloConfig cfg = new MiniAccumuloConfig(new File(args[0]), new String("secret"));
    cfg.setMemory(ServerType.TABLET_SERVER, 2, MemoryUnit.GIGABYTE);
    Map<String,String> site = new HashMap<String,String>();
    site.put(Property.TSERV_DATACACHE_SIZE.getKey(), "768M");
    site.put(Property.TSERV_INDEXCACHE_SIZE.getKey(), "256M");
    cfg.setSiteConfig(site);

    MiniAccumuloCluster cluster = new MiniAccumuloCluster(cfg);
    cluster.start();

    AccismusProperties aprops = new AccismusProperties();
    aprops.setAccumuloInstance(cluster.getInstanceName());
    aprops.setAccumuloUser("root");
    aprops.setAccumuloPassword("secret");
    aprops.setZookeeperRoot("/accismus");
    aprops.setZookeepers(cluster.getZooKeepers());

    InitializationProperties props = new InitializationProperties(aprops);
    props.setAccumuloTable("data");
    props.setNumThreads(40);

    Map<Column,String> observers = new HashMap<Column,String>();
    observers.put(INDEX_CHECK_COL, PhraseCounter.class.getName());
    observers.put(EXPORT_CHECK_COL, PhraseExporter.class.getName());
    props.setObservers(observers);

    Admin.initialize(props);

    MiniAccismus miniAccismus = new MiniAccismus(props);
    miniAccismus.start();

    Writer fw = new BufferedWriter(new FileWriter(new File(args[1])));
    aprops.store(fw, null);
    fw.close();

    System.out.println();
    System.out.println("Wrote : " + args[1]);
  }
}
