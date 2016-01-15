package phrasecount.pojos;

import com.google.common.base.Objects;

public class Counts {
  // number of documents a phrase was seen in
  public final long docPhraseCount;
  // total times a phrase was seen in all documents
  public final long totalPhraseCount;

  public Counts() {
    docPhraseCount = 0;
    totalPhraseCount = 0;
  }

  public Counts(long docPhraseCount, long totalPhraseCount) {
    this.docPhraseCount = docPhraseCount;
    this.totalPhraseCount = totalPhraseCount;
  }

  public Counts add(Counts other) {
    return new Counts(this.docPhraseCount + other.docPhraseCount, this.totalPhraseCount + other.totalPhraseCount);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Counts) {
      Counts opc = (Counts) o;
      return opc.docPhraseCount == docPhraseCount && opc.totalPhraseCount == totalPhraseCount;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return (int) (993 * totalPhraseCount + 17 * docPhraseCount);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("documents", docPhraseCount).add("total", totalPhraseCount).toString();
  }
}
