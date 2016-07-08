package phrasecount.cmd;

import java.io.File;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.hadoop.io.Text;

/**
 * Utility to add splits to the Accumulo table used by Fluo.
 */
public class Split {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage : " + Split.class.getName() + " <fluo props file> <table name>");
      System.exit(-1);
    }

    FluoConfiguration fluoConfig = new FluoConfiguration(new File(args[0]));
    ZooKeeperInstance zki =
        new ZooKeeperInstance(fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers());
    Connector conn = zki.getConnector(fluoConfig.getAccumuloUser(),
        new PasswordToken(fluoConfig.getAccumuloPassword()));

    SortedSet<Text> splits = new TreeSet<>();

    for (char c = 'b'; c < 'z'; c++) {
      splits.add(new Text("phrase:" + c));
    }

    conn.tableOperations().addSplits(args[1], splits);

    // TODO figure what threads are hanging around
    System.exit(0);
  }
}
