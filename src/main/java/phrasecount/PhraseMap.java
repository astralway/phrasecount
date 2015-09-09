package phrasecount;

import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.observer.Observer.Context;
import io.fluo.recipes.export.Export;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.map.CollisionFreeMap;
import io.fluo.recipes.map.Combiner;
import io.fluo.recipes.map.Update;
import io.fluo.recipes.map.UpdateObserver;
import phrasecount.pojos.Counts;

import static phrasecount.Constants.EXPORT_QUEUE_ID;

/**
 * This class contains all of the code related to the {@link CollisionFreeMap} that keeps track of phrase counts.
 */

public class PhraseMap {

  /**
   * A combiner for the {@link CollisionFreeMap} that stores phrase counts. The
   * {@link CollisionFreeMap} calls this combiner when it lazily updates the counts for a phrase.
   */
  public static class PcmCombiner implements Combiner<String, Counts> {

    @Override
    public Optional<Counts> combine(String key, Iterator<Counts> updates) {
      Counts sum = new Counts(0, 0);
      while (updates.hasNext()) {
        sum = sum.add(updates.next());
      }

      return Optional.of(sum);
    }
  }

  /**
   * This class is notified when the {@link CollisionFreeMap} used to store phrase counts updates a
   * phrase count. Updates are placed an Accumulo export queue to be exported to the table storing
   * phrase counts for query.
   */

  public static class PcmUpdateObserver extends UpdateObserver<String, Counts> {

    private ExportQueue<String, Counts> pcEq;

    @Override
    public void init(String mapId, Context observerContext) throws Exception {
      pcEq = ExportQueue.getInstance(EXPORT_QUEUE_ID, observerContext.getAppConfiguration());
    }

    @Override
    public void updatingValues(TransactionBase tx, Iterator<Update<String, Counts>> updates) {
      Iterator<Export<String, Counts>> exports = Iterators.transform(updates,
          new Function<Update<String, Counts>, Export<String, Counts>>() {
            @Override
            public Export<String, Counts> apply(Update<String, Counts> update) {
              return new Export<String, Counts>(update.getKey(), update.getNewValue().get());
            }
          });

      pcEq.addAll(tx, exports);
    }
  }

}
