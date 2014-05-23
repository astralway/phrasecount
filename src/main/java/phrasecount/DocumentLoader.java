package phrasecount;

import static phrasecount.Constants.DOC_CONTENT_COL;
import static phrasecount.Constants.DOC_HASH_COL;
import static phrasecount.Constants.DOC_REF_COUNT_COL;
import static phrasecount.Constants.INDEX_CHECK_COL;
import static phrasecount.Constants.TYPEL;
import accismus.api.Loader;
import accismus.api.Transaction;
import accismus.api.types.TypedTransaction;

public class DocumentLoader implements Loader {

  private Document document;

  public DocumentLoader(Document doc) {
    this.document = doc;
  }
  
  public void load(Transaction tx) throws Exception {

    TypedTransaction ttx = TYPEL.transaction(tx);
    String storedHash = ttx.get().row("uri:" + document.getURI()).col(DOC_HASH_COL).toString();

    if (storedHash == null || !storedHash.equals(document.getHash())) {

      ttx.set().row("uri:" + document.getURI()).col(DOC_HASH_COL).val(document.getHash());

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

  private void setRefCount(TypedTransaction tx, String hash, int rc) {
    tx.set().row("doc:" + hash).col(DOC_REF_COUNT_COL).val(rc);
    // TODO want to trigger checking for indexing in all cases when the ref count transitions to 0 or 1, except when it transitions from 2 to 1.... the
    // following transitions should trigger a check for indexing
    // null->1
    // 0->1
    // 1->0
    if (rc == 0 || rc == 1)
      tx.set().row("doc:" + hash).col(INDEX_CHECK_COL).val(); // setting this triggers the indexing observer
  }

  private void decrementRefCount(TypedTransaction tx, String hash) throws Exception {
    int rc = tx.get().row("doc:" + hash).col(DOC_REF_COUNT_COL).toInteger();
    setRefCount(tx, hash, rc - 1);
  }

  private void addNewDocument(TypedTransaction tx, Document doc) {
    setRefCount(tx, doc.getHash(), 1);
    tx.set().row("doc:" + doc.getHash()).col(DOC_CONTENT_COL).val(doc.getContent());
  }
}
