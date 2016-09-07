package phrasecount;

import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.recipes.accumulo.export.AccumuloExporter;
import org.apache.fluo.recipes.core.export.SequencedExport;
import phrasecount.pojos.Counts;
import phrasecount.query.PhraseCountTable;

/**
 * Export code that converts {@link Counts} objects from the export queue to Mutations that are
 * written to Accumulo.
 */
public class PhraseExporter extends AccumuloExporter<String, Counts> {

  @Override
  protected Collection<Mutation> translate(SequencedExport<String, Counts> export) {
    String phrase = export.getKey();
    long seq = export.getSequence();
    Counts counts = export.getValue();
    return Collections.singletonList(PhraseCountTable.createMutation(phrase, seq, counts));
  }
}
