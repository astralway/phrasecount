package phrasecount.cmd;

import static phrasecount.Constants.EXPORT_CHECK_COL;
import static phrasecount.Constants.INDEX_CHECK_COL;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;


public class Mini {

  static class Parameters {
    @Parameter(names = {"-m", "--moreMemory"}, description = "Use more memory")
    boolean moreMemory = false;

    @Parameter(names = {"-w", "--workerThreads"}, description = "Number of worker threads")
    int workerThreads = 5;

    @Parameter(names = {"-z", "--zookeeperPort"}, description = "Port to use for zookeeper")
    int zookeeperPort = 0;

    @Parameter(description = "<MAC dir> <output props file>")
    List<String> args;
  }

  public static void main(String[] args) throws Exception {

    Parameters params = new Parameters();
    JCommander jc = new JCommander(params);

    try {
      jc.parse(args);
    } catch (ParameterException pe) {
      System.out.println(pe.getMessage());
      jc.setProgramName(Mini.class.getSimpleName());
      jc.usage();
      System.exit(-1);
    }

    MiniAccumuloConfig cfg = new MiniAccumuloConfig(new File(params.args.get(0)), new String("secret"));
    cfg.setZooKeeperPort(params.zookeeperPort);
    if (params.moreMemory) {
      cfg.setMemory(ServerType.TABLET_SERVER, 2, MemoryUnit.GIGABYTE);
      Map<String,String> site = new HashMap<String,String>();
      site.put(Property.TSERV_DATACACHE_SIZE.getKey(), "768M");
      site.put(Property.TSERV_INDEXCACHE_SIZE.getKey(), "256M");
      cfg.setSiteConfig(site);
    }

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
    props.setNumThreads(params.workerThreads);

    Map<Column,String> observers = new HashMap<Column,String>();
    observers.put(INDEX_CHECK_COL, PhraseCounter.class.getName());
    observers.put(EXPORT_CHECK_COL, PhraseExporter.class.getName());
    props.setObservers(observers);

    Admin.initialize(props);

    MiniAccismus miniAccismus = new MiniAccismus(props);
    miniAccismus.start();

    Writer fw = new BufferedWriter(new FileWriter(new File(params.args.get(1))));
    aprops.store(fw, null);
    fw.close();

    System.out.println();
    System.out.println("Wrote : " + params.args.get(1));
  }
}
