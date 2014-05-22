package phrasecount.cmd;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Collections;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;

import phrasecount.DocumentIndexer;
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
    props.setNumThreads(5);
    props.setObservers(Collections.singletonMap(new Column("index", "check"), DocumentIndexer.class.getName()));

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
