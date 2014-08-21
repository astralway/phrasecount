package phrasecount.cmd;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A utility to compare the phrase counts stored in the Fluo table to those stored in the export table.
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
      System.err.println("Usage : " + Compare.class.getName() + " <fluo props file> <fluo table name> <export table name>");
      System.exit(-1);
    }

    FluoConfiguration props = new FluoConfiguration(new File(args[0]));

    try (FluoClient fluoClient = FluoFactory.newClient(props)) {
      Snapshot snap = fluoClient.newSnapshot();

      PeekingIterator<PhraseCount> fluoIter = Iterators.peekingIterator(Print.createPhraseIterator(snap));
      PeekingIterator<PhraseCount> accumuloIter = Iterators.peekingIterator(createPhraseIterator(props, args[2]));

      int sameCount = 0;

      while (accumuloIter.hasNext() && fluoIter.hasNext()) {
        PhraseCount accumuloPhrase = accumuloIter.peek();
        PhraseCount fluoPhrase = fluoIter.peek();

        if (fluoPhrase.equals(accumuloPhrase)) {
          fluoIter.next();
          accumuloIter.next();
          sameCount++;
          continue;
        }

        int comp = fluoPhrase.phrase.compareTo(accumuloPhrase.phrase);

        if (comp == 0) {
          fluoIter.next();
          accumuloIter.next();
          System.out.printf("Counts differ    : %7d %7d %7d %7d '%s'\n", fluoPhrase.docCount, fluoPhrase.sum, accumuloPhrase.docCount,
              accumuloPhrase.sum, fluoPhrase.phrase);
        }
        if (comp < 0) {
          System.out.printf("Only in Fluo : %7d %7d '%s'\n", fluoPhrase.docCount, fluoPhrase.sum, fluoPhrase.phrase);
          fluoIter.next();
        } else {
          System.out.printf("Only in Accumulo : %7d %7d '%s'\n", accumuloPhrase.docCount, accumuloPhrase.sum, accumuloPhrase.phrase);
          accumuloIter.next();
        }
      }

      while (accumuloIter.hasNext()) {
        PhraseCount accumuloPhrase = accumuloIter.next();
        System.out.printf("Only in Accumulo : %7d %7d '%s'\n", accumuloPhrase.docCount, accumuloPhrase.sum, accumuloPhrase.phrase);
      }

      while (fluoIter.hasNext()) {
        PhraseCount fluoPhrase = fluoIter.next();
        System.out.printf("Only in Fluo : %7d %7d '%s'\n", fluoPhrase.docCount, fluoPhrase.sum, fluoPhrase.phrase);
      }

      System.out.println();
      System.out.println("same count : " + sameCount);
    }
    // TODO figure what threads are hanging around
    System.exit(0);
  }

  static Iterator<PhraseCount> createPhraseIterator(FluoConfiguration fluoConfig, String table) throws FileNotFoundException, IOException,
      AccumuloException,
      AccumuloSecurityException, TableNotFoundException {

    ZooKeeperInstance zki = new ZooKeeperInstance(fluoConfig.getAccumuloInstance(), fluoConfig.getZookeepers());
    Connector conn = zki.getConnector(fluoConfig.getAccumuloUser(), new PasswordToken(fluoConfig.getAccumuloPassword()));

    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
    scanner.fetchColumn(new Text("stat"), new Text("sum"));
    scanner.fetchColumn(new Text("stat"), new Text("docCount"));

    Iterator<PhraseCount> accumuloIter = Iterators.transform(new RowIterator(scanner), new AccumuloTransform());
    return accumuloIter;
  }
}
