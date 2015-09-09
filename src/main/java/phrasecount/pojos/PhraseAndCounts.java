package phrasecount.pojos;

public class PhraseAndCounts extends Counts {
  public String phrase;

  public PhraseAndCounts(String phrase, int docPhraseCount, int totalPhraseCount) {
    super(docPhraseCount, totalPhraseCount);
    this.phrase = phrase;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PhraseAndCounts) {
      PhraseAndCounts op = (PhraseAndCounts) o;
      return phrase.equals(op.phrase) && super.equals(op);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode() + 31 * phrase.hashCode();
  }
}
