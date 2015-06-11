package phrasecount;

import io.fluo.api.client.Loader;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.types.TypedTransactionBase;

import static phrasecount.Constants.DOC_CONTENT_COL;
import static phrasecount.Constants.DOC_HASH_COL;
import static phrasecount.Constants.DOC_REF_COUNT_COL;
import static phrasecount.Constants.INDEX_CHECK_COL;
import static phrasecount.Constants.TYPEL;

/**
 * Executes document load transactions which dedupe and reference count documents.  IF needed, the observer that updates phrase counts is triggered.
 */

public class DocumentLoader implements Loader {

  private Document document;

  public DocumentLoader(Document doc) {
    this.document = doc;
  }

  @Override
  public void load(TransactionBase tx, Context context) throws Exception {

    // TODO need a strategy for dealing w/ large documents. If a worker processes many large documents concurrently it could cause memory exhaustion. . Could
    // large documents up into pieces, however not sure if the example should be complicated w/ this.

    TypedTransactionBase ttx = TYPEL.wrap(tx);
    String storedHash = ttx.get().row("uri:" + document.getURI()).col(DOC_HASH_COL).toString();

    if (storedHash == null || !storedHash.equals(document.getHash())) {

      ttx.mutate().row("uri:" + document.getURI()).col(DOC_HASH_COL).set(document.getHash());

      Integer refCount = ttx.get().row("doc:" + document.getHash()).col(DOC_REF_COUNT_COL).toInteger();
      if (refCount == null) {
        // this document was never seen before
        addNewDocument(ttx, document);
      } else {
        setRefCount(ttx, document.getHash(), refCount + 1);
      }

      if (storedHash != null)
        decrementRefCount(ttx, storedHash);
    }
  }

  private void setRefCount(TypedTransactionBase tx, String hash, int rc) {
    tx.mutate().row("doc:" + hash).col(DOC_REF_COUNT_COL).set(rc);
    // TODO want to trigger checking for indexing in all cases when the ref count transitions to 0 or 1, except when it transitions from 2 to 1.... the
    // following transitions should trigger a check for indexing
    // null->1
    // 0->1
    // 1->0
    if (rc == 0 || rc == 1)
      tx.mutate().row("doc:" + hash).col(INDEX_CHECK_COL).set(); // setting this triggers the phrase counting observer
  }

  private void decrementRefCount(TypedTransactionBase tx, String hash) throws Exception {
    int rc = tx.get().row("doc:" + hash).col(DOC_REF_COUNT_COL).toInteger();
    setRefCount(tx, hash, rc - 1);
  }

  private void addNewDocument(TypedTransactionBase tx, Document doc) {
    setRefCount(tx, doc.getHash(), 1);
    tx.mutate().row("doc:" + doc.getHash()).col(DOC_CONTENT_COL).set(doc.getContent());
  }
}
