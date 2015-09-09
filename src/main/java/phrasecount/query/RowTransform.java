package phrasecount.query;

import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import phrasecount.pojos.PhraseAndCounts;

public class RowTransform implements Function<Iterator<Entry<Key, Value>>, PhraseAndCounts> {
  @Override
  public PhraseAndCounts apply(Iterator<Entry<Key, Value>> input) {
    String phrase = null;

    int totalPhraseCount = 0;
    int docPhraseCount = 0;

    while (input.hasNext()) {
      Entry<Key, Value> colEntry = input.next();
      String cq = colEntry.getKey().getColumnQualifierData().toString();

      if (cq.equals(PhraseCountTable.TOTAL_PC_CQ))
        totalPhraseCount = Integer.parseInt(colEntry.getValue().toString());
      else
        docPhraseCount = Integer.parseInt(colEntry.getValue().toString());

      if (phrase == null)
        phrase = colEntry.getKey().getRowData().toString();
    }

    return new PhraseAndCounts(phrase, docPhraseCount, totalPhraseCount);
  }
}
