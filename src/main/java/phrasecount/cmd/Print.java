package phrasecount.cmd;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;

import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;

import phrasecount.Constants;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class Print {

  static class PhraseCount {
    String phrase;
    int sum;
    int docCount;

    PhraseCount(String phrase, int sum, int docCount) {
      this.phrase = phrase;
      this.sum = sum;
      this.docCount = docCount;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof PhraseCount) {
        PhraseCount op = (PhraseCount) o;

        return phrase.equals(op.phrase) && sum == op.sum && docCount == op.docCount;
      }

      return false;
    }
  }

  static class PhraseRowTransform implements Function<Entry<Bytes,ColumnIterator>,PhraseCount> {

    @Override
    public PhraseCount apply(Entry<Bytes,ColumnIterator> input) {
      String phrase = input.getKey().toString().substring(7);

      int sum = 0;
      int docCount = 0;

      ColumnIterator citer = input.getValue();
      while (citer.hasNext()) {
        Entry<Column,Bytes> colEntry = citer.next();
        String cq = colEntry.getKey().getQualifier().toString();

        if (cq.equals("sum"))
          sum = Integer.parseInt(colEntry.getValue().toString());
        else
          docCount = Integer.parseInt(colEntry.getValue().toString());
      }

      return new PhraseCount(phrase, sum, docCount);
    }

  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage : " + Print.class.getName() + " <fluo props file>");
      System.exit(-1);
    }

    try (FluoClient fluoClient = FluoFactory.newClient(new FluoConfiguration(new File(args[0]))); Snapshot snap = fluoClient.newSnapshot()) {
      Iterator<PhraseCount> phraseIter = createPhraseIterator(snap);

      while (phraseIter.hasNext()) {
        PhraseCount phraseCount = phraseIter.next();
        System.out.printf("%7d %7d '%s'\n", phraseCount.docCount, phraseCount.sum, phraseCount.phrase);
      }

      // TODO could precompute this using observers
      int uriCount = count(snap, "uri:", Constants.DOC_HASH_COL);
      int documentCount = count(snap, "doc:", Constants.DOC_REF_COUNT_COL);
      int numIndexedDocs = count(snap, "doc:", Constants.INDEX_STATUS_COL);

      System.out.println();
      System.out.printf("# uris                : %,d\n", uriCount);
      System.out.printf("# unique documents    : %,d\n", documentCount);
      System.out.printf("# processed documents : %,d\n", numIndexedDocs);
      System.out.println();
    }

    // TODO figure what threads are hanging around
    System.exit(0);
  }

  private static int count(Snapshot snap, String prefix, Column col) throws Exception {
    ScannerConfiguration scanConfig = new ScannerConfiguration();
    scanConfig.setSpan(Span.prefix(prefix));
    scanConfig.fetchColumn(col.getFamily(), col.getQualifier());

    int count = 0;

    RowIterator riter = snap.get(scanConfig);
    while (riter.hasNext()) {
      @SuppressWarnings("unused")
      Entry<Bytes,ColumnIterator> rowEntry = riter.next();
      count++;
    }

    return count;
  }

  static Iterator<PhraseCount> createPhraseIterator(Snapshot snap) throws Exception {
    ScannerConfiguration scanConfig = new ScannerConfiguration();
    scanConfig.setSpan(Span.prefix("phrase:"));
    scanConfig.fetchColumn(Constants.STAT_SUM_COL.getFamily(), Constants.STAT_SUM_COL.getQualifier());
    scanConfig.fetchColumn(Constants.STAT_DOC_COUNT_COL.getFamily(), Constants.STAT_DOC_COUNT_COL.getQualifier());

    Iterator<PhraseCount> phraseIter = Iterators.transform(snap.get(scanConfig), new PhraseRowTransform());
    return phraseIter;
  }

}
