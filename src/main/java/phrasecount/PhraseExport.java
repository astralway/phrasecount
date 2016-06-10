package phrasecount;

import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.recipes.accumulo.export.AccumuloExport;
import phrasecount.pojos.Counts;
import phrasecount.query.PhraseCountTable;

/**
 * Glue code to convert {@link Counts} objects from the export queue to Mutations to write to Accumulo.
 */
public class PhraseExport implements AccumuloExport<String> {

  private Counts pc;

  public PhraseExport(){}

  public PhraseExport(Counts pc){
    this.pc = pc;
  }

  @Override
  public Collection<Mutation> toMutations(String phrase, long seq) {
    return Collections.singletonList(PhraseCountTable.createMutation(phrase, seq, pc));
  }
}
