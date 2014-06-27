package phrasecount.cmd;

import static accismus.api.config.ConnectionProperties.ACCUMULO_INSTANCE_PROP;
import static accismus.api.config.ConnectionProperties.ACCUMULO_PASSWORD_PROP;
import static accismus.api.config.ConnectionProperties.ACCUMULO_USER_PROP;
import static accismus.api.config.ConnectionProperties.ZOOKEEPER_CONNECT_PROP;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import phrasecount.cmd.Print.PhraseCount;
import accismus.api.Snapshot;
import accismus.api.SnapshotFactory;
import accismus.api.config.ConnectionProperties;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A utility to compare the phrase counts stored in the Accismus table to those stored in the export table.
 */

public class Compare {

  static class AccumuloTransform implements Function<Iterator<Entry<Key,Value>>,PhraseCount> {

    @Override
    public PhraseCount apply(Iterator<Entry<Key,Value>> input) {
      String phrase = null;

      int sum = 0;
      int docCount = 0;

      while (input.hasNext()) {
        Entry<Key,Value> colEntry = input.next();
        String cq = colEntry.getKey().getColumnQualifierData().toString();

        if (cq.equals("sum"))
          sum = Integer.parseInt(colEntry.getValue().toString());
        else
          docCount = Integer.parseInt(colEntry.getValue().toString());

        if (phrase == null)
          phrase = colEntry.getKey().getRowData().toString();
      }

      return new PhraseCount(phrase, sum, docCount);
    }

  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage : " + Compare.class.getName() + " <accismus props file> <accismus table name> <export table name>");
      System.exit(-1);
    }

    Properties props = new ConnectionProperties(new File(args[0]));

    SnapshotFactory snapFact = new SnapshotFactory(props);
    try {
      Snapshot snap = snapFact.createSnapshot();

      PeekingIterator<PhraseCount> accismusIter = Iterators.peekingIterator(Print.createPhraseIterator(snap));
      PeekingIterator<PhraseCount> accumuloIter = Iterators.peekingIterator(createPhraseIterator(props, args[2]));

      while (accumuloIter.hasNext() && accismusIter.hasNext()) {
        PhraseCount accumuloPhrase = accumuloIter.peek();
        PhraseCount accismusPhrase = accismusIter.peek();

        if (accismusPhrase.equals(accumuloPhrase)) {
          accismusIter.next();
          accumuloIter.next();
          continue;
        }

        int comp = accismusPhrase.phrase.compareTo(accumuloPhrase.phrase);

        if (comp == 0) {
          accismusIter.next();
          accumuloIter.next();
          System.out.printf("Counts differ    : %7d %7d %7d %7d '%s'\n", accismusPhrase.docCount, accismusPhrase.sum, accumuloPhrase.docCount,
              accumuloPhrase.sum, accismusPhrase.phrase);
        }
        if (comp < 0) {
          System.out.printf("Only in Accismus : %7d %7d '%s'\n", accismusPhrase.docCount, accismusPhrase.sum, accismusPhrase.phrase);
          accismusIter.next();
        } else {
          System.out.printf("Only in Accumulo : %7d %7d '%s'\n", accumuloPhrase.docCount, accumuloPhrase.sum, accumuloPhrase.phrase);
          accumuloIter.next();
        }
      }

      while (accumuloIter.hasNext()) {
        PhraseCount accumuloPhrase = accumuloIter.next();
        System.out.printf("Only in Accumulo : %7d %7d '%s'\n", accumuloPhrase.docCount, accumuloPhrase.sum, accumuloPhrase.phrase);
      }

      while (accismusIter.hasNext()) {
        PhraseCount accismusPhrase = accismusIter.next();
        System.out.printf("Only in Accismus : %7d %7d '%s'\n", accismusPhrase.docCount, accismusPhrase.sum, accismusPhrase.phrase);
      }
    } finally {
      snapFact.close();
    }
    // TODO figure what threads are hanging around
    System.exit(0);
  }

  static Iterator<PhraseCount> createPhraseIterator(Properties props, String table) throws FileNotFoundException, IOException, AccumuloException,
      AccumuloSecurityException, TableNotFoundException {

    ZooKeeperInstance zki = new ZooKeeperInstance(props.getProperty(ACCUMULO_INSTANCE_PROP), props.getProperty(ZOOKEEPER_CONNECT_PROP));
    Connector conn = zki.getConnector(props.getProperty(ACCUMULO_USER_PROP), new PasswordToken(props.getProperty(ACCUMULO_PASSWORD_PROP)));

    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
    scanner.fetchColumn(new Text("stat"), new Text("sum"));
    scanner.fetchColumn(new Text("stat"), new Text("docCount"));

    Iterator<PhraseCount> accumuloIter = Iterators.transform(new RowIterator(scanner), new AccumuloTransform());
    return accumuloIter;
  }
}
