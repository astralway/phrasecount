package phrasecount;

import java.util.Iterator;
import java.util.Optional;

import com.google.common.collect.Iterators;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.observer.Observer.Context;
import org.apache.fluo.recipes.core.export.Export;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.map.CollisionFreeMap;
import org.apache.fluo.recipes.core.map.Combiner;
import org.apache.fluo.recipes.core.map.Update;
import org.apache.fluo.recipes.core.map.UpdateObserver;
import phrasecount.pojos.Counts;

import static phrasecount.Constants.EXPORT_QUEUE_ID;

/**
 * This class contains all of the code related to the {@link CollisionFreeMap} that keeps track of
 * phrase counts.
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
      Iterator<Export<String, Counts>> exports =
          Iterators.transform(updates, u -> new Export<>(u.getKey(), u.getNewValue().get()));
      pcEq.addAll(tx, exports);
    }
  }

}
