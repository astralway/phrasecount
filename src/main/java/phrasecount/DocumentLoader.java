package phrasecount;

import accismus.api.Column;
import accismus.api.Loader;
import accismus.api.Transaction;
import accismus.api.types.StringEncoder;
import accismus.api.types.TypedTransaction;

public class DocumentLoader implements Loader {

  private Document document;

  public DocumentLoader(Document doc) {
    this.document = doc;
  }
  
  public void load(Transaction tx) throws Exception {

    TypedTransaction ttx = new TypedTransaction(tx, new StringEncoder());
    String storedHash = ttx.getd("uri:" + document.getURI(), new Column("doc", "hash")).toString();

    if (storedHash == null || !storedHash.equals(document.getHash())) {

      ttx.sete("uri:" + document.getURI(), new Column("doc", "hash")).from(document.getHash());

      Integer refCount = ttx.getd("doc:" + document.getHash(), new Column("doc", "refCount")).toInteger();
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
    tx.sete("doc:" + hash, new Column("doc", "refCount")).from(rc);
    // TODO want to trigger checking for indexing in all cases when the ref count transitions to 0 or 1, except when it transitions from 2 to 1.... the
    // following transitions should trigger a check for indexing
    // null->1
    // 0->1
    // 1->0
    if (rc == 0 || rc == 1)
      tx.sete("doc:" + hash, new Column("index", "check")).from(""); // setting this triggers the indexing observer
  }

  private void decrementRefCount(TypedTransaction tx, String hash) throws Exception {
    int rc = tx.getd("doc:" + hash, new Column("doc", "refCount")).toInteger();
    setRefCount(tx, hash, rc - 1);
  }

  private void addNewDocument(TypedTransaction tx, Document doc) {
    setRefCount(tx, doc.getHash(), 1);
    tx.sete("doc:" + doc.getHash(), new Column("doc", "content")).from(doc.getContent());
  }
}
