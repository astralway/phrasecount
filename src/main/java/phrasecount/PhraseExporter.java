package phrasecount;

import java.util.function.Consumer;

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
  protected void translate(SequencedExport<String, Counts> export, Consumer<Mutation> consumer) {
    String phrase = export.getKey();
    long seq = export.getSequence();
    Counts counts = export.getValue();
    consumer.accept(PhraseCountTable.createMutation(phrase, seq, counts));
  }
}
