package phrasecount;

import java.util.Collections;
import java.util.List;

import io.fluo.recipes.accumulo.export.AccumuloExporter;
import org.apache.accumulo.core.data.Mutation;
import phrasecount.pojos.Counts;
import phrasecount.query.PhraseCountTable;

/**
 * Glue code to convert {@link Counts} objects from the export queue to Mutations to write to Accumulo.
 */
public class PhraseExporter extends AccumuloExporter<String, Counts> {
  @Override
  protected List<Mutation> convert(String phrase, long seq, Counts pc) {
    return Collections.singletonList(PhraseCountTable.createMutation(phrase, seq, pc));
  }
}
