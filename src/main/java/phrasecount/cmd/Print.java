package phrasecount.cmd;

import java.io.File;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;

import phrasecount.Constants;
import accismus.api.Column;
import accismus.api.ColumnIterator;
import accismus.api.RowIterator;
import accismus.api.ScannerConfiguration;
import accismus.api.Snapshot;
import accismus.api.SnapshotFactory;
import accismus.api.config.AccismusProperties;

public class Print {

  private static int count(Snapshot snap, String prefix, Column col) throws Exception {
    ScannerConfiguration scanConfig = new ScannerConfiguration();
    scanConfig.setRange(Range.prefix(prefix));
    scanConfig.fetchColumn(col.getFamily(), col.getQualifier());

    int count = 0;

    RowIterator riter = snap.get(scanConfig);
    while (riter.hasNext()) {
      @SuppressWarnings("unused")
      Entry<ByteSequence,ColumnIterator> rowEntry = riter.next();
      count++;
    }

    return count;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage : " + Print.class.getName() + " <accismus props file>");
      System.exit(-1);
    }

    SnapshotFactory snapFact = new SnapshotFactory(new AccismusProperties(new File(args[0])));
    Snapshot snap = snapFact.createSnapshot();
    try {
      ScannerConfiguration scanConfig = new ScannerConfiguration();
      scanConfig.setRange(Range.prefix("phrase:"));
      scanConfig.fetchColumn(Constants.STAT_SUM_COL.getFamily(), Constants.STAT_SUM_COL.getQualifier());
      scanConfig.fetchColumn(Constants.STAT_DOC_COUNT_COL.getFamily(), Constants.STAT_DOC_COUNT_COL.getQualifier());

      //TODO make TypedSnapshot support this
      RowIterator riter = snap.get(scanConfig);
      while (riter.hasNext()) {
        Entry<ByteSequence,ColumnIterator> rowEntry = riter.next();
        String phrase = rowEntry.getKey().toString().substring(7);
        
        int sum = 0;
        int docCount = 0;
        
        ColumnIterator citer = rowEntry.getValue();
        while(citer.hasNext()){
          Entry<Column,ByteSequence> colEntry = citer.next();
          String cq = colEntry.getKey().getQualifier().toString();

          if (cq.equals("sum"))
            sum = Integer.parseInt(colEntry.getValue().toString());
          else
            docCount = Integer.parseInt(colEntry.getValue().toString());
        }

        System.out.printf("%7d %7d '%s'\n", docCount, sum, phrase);
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


    } finally {
      snapFact.close();
    }

    // TODO figure what threads are hanging around
    System.exit(0);
  }

}
