package phrasecount.cmd;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.fluo.api.client.FluoAdmin.InitOpts;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.mini.MiniFluo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.commons.configuration.PropertiesConfiguration;
import phrasecount.HCCounter;
import phrasecount.PhraseCounter;
import phrasecount.PhraseExporter;


public class Mini {

  static class Parameters {
    @Parameter(names = {"-m", "--moreMemory"}, description = "Use more memory")
    boolean moreMemory = false;

    @Parameter(names = {"-w", "--workerThreads"}, description = "Number of worker threads")
    int workerThreads = 5;

    @Parameter(names = {"-t", "--tabletServers"}, description = "Number of tablet servers")
    int tabletServers = 2;

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
      if (params.args == null || params.args.size() != 2)
        throw new ParameterException("Expected two arguments");
    } catch (ParameterException pe) {
      System.out.println(pe.getMessage());
      jc.setProgramName(Mini.class.getSimpleName());
      jc.usage();
      System.exit(-1);
    }

    MiniAccumuloConfig cfg = new MiniAccumuloConfig(new File(params.args.get(0)), new String("secret"));
    cfg.setZooKeeperPort(params.zookeeperPort);
    cfg.setNumTservers(params.tabletServers);
    if (params.moreMemory) {
      cfg.setMemory(ServerType.TABLET_SERVER, 2, MemoryUnit.GIGABYTE);
      Map<String,String> site = new HashMap<String,String>();
      site.put(Property.TSERV_DATACACHE_SIZE.getKey(), "768M");
      site.put(Property.TSERV_INDEXCACHE_SIZE.getKey(), "256M");
      cfg.setSiteConfig(site);
    }

    MiniAccumuloCluster cluster = new MiniAccumuloCluster(cfg);
    cluster.start();

    FluoConfiguration fluoConfig = new FluoConfiguration();

    fluoConfig.setMiniStartAccumulo(false);
    fluoConfig.setAccumuloInstance(cluster.getInstanceName());
    fluoConfig.setAccumuloUser("root");
    fluoConfig.setAccumuloPassword("secret");
    fluoConfig.setAccumuloZookeepers(cluster.getZooKeepers());
    fluoConfig.setInstanceZookeepers(cluster.getZooKeepers()+"/fluo");


    fluoConfig.setAccumuloTable("data");
    fluoConfig.setWorkerThreads(params.workerThreads);

    fluoConfig.setApplicationName("phrasecount");

    List<ObserverConfiguration> observers = new ArrayList<ObserverConfiguration>();
    observers.add(new ObserverConfiguration(PhraseCounter.class.getName()));

    Map<String,String> exportConfig = setupAccumuloExport(cluster);
    observers.add(new ObserverConfiguration(PhraseExporter.class.getName()).setParameters(exportConfig));

    observers.add(new ObserverConfiguration(HCCounter.class.getName()));
    fluoConfig.setObservers(observers);

    FluoFactory.newAdmin(fluoConfig).initialize(new InitOpts());

    MiniFluo miniFluo = FluoFactory.newMiniFluo(fluoConfig);

    PropertiesConfiguration propsConfig = new PropertiesConfiguration();
    propsConfig.copy(miniFluo.getClientConfiguration());

    propsConfig.save(params.args.get(1));

    System.out.println();
    System.out.println("Wrote : " + params.args.get(1));
  }

  private static Map<String,String> setupAccumuloExport(MiniAccumuloCluster cluster) throws Exception {
    Connector conn = cluster.getConnector("root", "secret");
    conn.tableOperations().create("dataExport");

    Map<String,String> exportConfig = new HashMap<String,String>();

    exportConfig.put("sink", "accumulo");
    exportConfig.put("instance", cluster.getInstanceName());
    exportConfig.put("zookeepers", cluster.getZooKeepers());
    exportConfig.put("user", "root");
    exportConfig.put("password", "secret");
    exportConfig.put("table", "dataExport");

    return exportConfig;
  }
}
