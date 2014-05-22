package phrasecount;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;

import accismus.api.Column;
import accismus.api.Observer;
import accismus.api.Transaction;
import accismus.api.types.StringEncoder;
import accismus.api.types.TypedTransaction;

public class DocumentIndexer implements Observer {

  private static enum IndexStatus {
    INDEXED, UNINDEXED
  }

  public void process(Transaction tx, ByteSequence row, Column col) throws Exception {

    // check if the trigger row exist, if not an exception will thrown and the framework will check for collisions
    TypedTransaction ttx = new TypedTransaction(tx, new StringEncoder());
    ttx.get(row, col).equals("");

    IndexStatus status = getStatus(ttx, row);
    int refCount = ttx.getd(row, new Column("doc", "refCount")).toInteger();

    if (status == IndexStatus.UNINDEXED && refCount > 0) {
      indexDocument(ttx, row, 1);
      setStatus(ttx, row, IndexStatus.INDEXED);
    } else if (status == IndexStatus.INDEXED && refCount == 0) {
      indexDocument(ttx, row, -1);
      deleteDocument(ttx, row);
    }

    tx.delete(row, col);

  }

  private void deleteDocument(TypedTransaction tx, ByteSequence row) {
    // TODO it would probably be useful to have a deleteRow method on Transaction... this method could start off w/ a simple implementation and later be
    // optimized... or could have a delete range option

    // TODO this is brittle, this code assumes it knows all possible columns
    tx.delete(row, new Column("doc", "content"));
    tx.delete(row, new Column("doc", "refCount"));
    tx.delete(row, new Column("index", "status"));

  }

  private void indexDocument(TypedTransaction ttx, ByteSequence row, int multiplier) throws Exception {
    String content = ttx.get(row, new Column("doc", "content")).toString();

    // this makes the assumption that the implementation of getPhrases is invariant. This is probably a bad assumption. A possible way to make this more robust
    // is to store the output of getPhrases when indexing and use the stored output when unindexing. Alternatively, could store the version of Document used for
    // indexing.
    Set<Entry<String,Integer>> phrases = new Document(null, content).getPhrases().entrySet();

    for (Entry<String,Integer> entry : phrases) {
      String phrase = entry.getKey();
      String phraseRow = "phrase:" + phrase;
      Integer sum = ttx.getd(phraseRow, new Column("stat", "sum")).toInteger(0);
      Integer docCount = ttx.getd(phraseRow, new Column("stat", "docCount")).toInteger(0);

      int newSum = sum + multiplier * entry.getValue();
      int newDocCount = docCount + multiplier * 1;

      if (newSum > 0)
        ttx.sete(phraseRow, new Column("stat", "sum")).from(newSum);
      else
        ttx.delete(phraseRow, new Column("stat", "sum"));

      if (newDocCount > 0)
        ttx.sete(phraseRow, new Column("stat", "docCount")).from(newDocCount);
      else
        ttx.delete(phraseRow, new Column("stat", "docCount"));

    }

  }

  private IndexStatus getStatus(TypedTransaction tx, ByteSequence row) throws Exception {
    String status = tx.getd(row, new Column("index", "status")).toString();

    if (status == null)
      return IndexStatus.UNINDEXED;

    return IndexStatus.valueOf(status);
  }

  private void setStatus(TypedTransaction tx, ByteSequence row, IndexStatus status) throws Exception {
    tx.sete(row.toString(), new Column("index", "status")).from(status.name());
  }

}
