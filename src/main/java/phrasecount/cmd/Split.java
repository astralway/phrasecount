package phrasecount.cmd;

import static io.fluo.api.config.ConnectionProperties.ACCUMULO_INSTANCE_PROP;
import static io.fluo.api.config.ConnectionProperties.ACCUMULO_PASSWORD_PROP;
import static io.fluo.api.config.ConnectionProperties.ACCUMULO_USER_PROP;
import static io.fluo.api.config.ConnectionProperties.ZOOKEEPER_CONNECT_PROP;
import io.fluo.api.config.ConnectionProperties;

import java.io.File;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;


/**
 * Utiltiy to add splits to the Accumulo table used by Fluo.
 */

public class Split {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage : " + Split.class.getName() + " <fluo props file> <table name>");
      System.exit(-1);
    }

    Properties props = new ConnectionProperties(new File(args[0]));
    ZooKeeperInstance zki = new ZooKeeperInstance(props.getProperty(ACCUMULO_INSTANCE_PROP), props.getProperty(ZOOKEEPER_CONNECT_PROP));
    Connector conn = zki.getConnector(props.getProperty(ACCUMULO_USER_PROP), new PasswordToken(props.getProperty(ACCUMULO_PASSWORD_PROP)));

    SortedSet<Text> splits = new TreeSet<Text>();

    for (char c = 'b'; c < 'z'; c++) {
      splits.add(new Text("phrase:" + c));
    }

    conn.tableOperations().addSplits(args[1], splits);

    // TODO figure what threads are hanging around
    System.exit(0);
  }
}
