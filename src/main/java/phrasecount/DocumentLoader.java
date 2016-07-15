package phrasecount;

import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;
import phrasecount.pojos.Document;

import static phrasecount.Constants.DOC_CONTENT_COL;
import static phrasecount.Constants.DOC_HASH_COL;
import static phrasecount.Constants.DOC_REF_COUNT_COL;
import static phrasecount.Constants.INDEX_CHECK_COL;
import static phrasecount.Constants.TYPEL;

/**
 * Executes document load transactions which dedupe and reference count documents. If needed, the
 * observer that updates phrase counts is triggered.
 */
public class DocumentLoader implements Loader {

  private Document document;

  public DocumentLoader(Document doc) {
    this.document = doc;
  }

  @Override
  public void load(TransactionBase tx, Context context) throws Exception {

    // TODO Need a strategy for dealing w/ large documents. If a worker processes many large
    // documents concurrently, it could cause memory exhaustion. Could break up large documents
    // into pieces, However, not sure if the example should be complicated with this.

    TypedTransactionBase ttx = TYPEL.wrap(tx);
    String storedHash = ttx.get().row("uri:" + document.getURI()).col(DOC_HASH_COL).toString();

    if (storedHash == null || !storedHash.equals(document.getHash())) {

      ttx.mutate().row("uri:" + document.getURI()).col(DOC_HASH_COL).set(document.getHash());

      Integer refCount =
          ttx.get().row("doc:" + document.getHash()).col(DOC_REF_COUNT_COL).toInteger();
      if (refCount == null) {
        // this document was never seen before
        addNewDocument(ttx, document);
      } else {
        setRefCount(ttx, document.getHash(), refCount, refCount + 1);
      }

      if (storedHash != null) {
        decrementRefCount(ttx, refCount, storedHash);
      }
    }
  }

  private void setRefCount(TypedTransactionBase tx, String hash, Integer prevRc, int rc) {
    tx.mutate().row("doc:" + hash).col(DOC_REF_COUNT_COL).set(rc);

    if (rc == 0 || (rc == 1 && (prevRc == null || prevRc == 0))) {
      // setting this triggers DocumentObserver
      tx.mutate().row("doc:" + hash).col(INDEX_CHECK_COL).set();
    }
  }

  private void decrementRefCount(TypedTransactionBase tx, Integer prevRc, String hash) {
    int rc = tx.get().row("doc:" + hash).col(DOC_REF_COUNT_COL).toInteger();
    setRefCount(tx, hash, prevRc, rc - 1);
  }

  private void addNewDocument(TypedTransactionBase tx, Document doc) {
    setRefCount(tx, doc.getHash(), null, 1);
    tx.mutate().row("doc:" + doc.getHash()).col(DOC_CONTENT_COL).set(doc.getContent());
  }
}
