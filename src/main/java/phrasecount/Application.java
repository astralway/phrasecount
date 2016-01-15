package phrasecount;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.accumulo.export.TableInfo;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.map.CollisionFreeMap;
import io.fluo.recipes.serialization.KryoSimplerSerializer;
import phrasecount.pojos.Counts;
import phrasecount.pojos.PcKryoFactory;

import static phrasecount.Constants.EXPORT_QUEUE_ID;
import static phrasecount.Constants.PCM_ID;

public class Application {

  public static class Options {
    public Options(int pcmBuckets, int eqBuckets, String instance, String zooKeepers, String user,
        String password, String eTable) {
      this.phraseCountMapBuckets = pcmBuckets;
      this.exportQueueBuckets = eqBuckets;
      this.instance = instance;
      this.zookeepers = zooKeepers;
      this.user = user;
      this.password = password;
      this.exportTable = eTable;

    }

    public int phraseCountMapBuckets;
    public int exportQueueBuckets;

    public String instance;
    public String zookeepers;
    public String user;
    public String password;
    public String exportTable;
  }

  /**
   * Sets Fluo configuration needed to run the phrase count application
   *
   * @param fluoConfig FluoConfiguration
   * @param opts Options
   */
  public static void configure(FluoConfiguration fluoConfig, Options opts) {
    // set up an observer that watches the reference counts of documents. When a document is
    // referenced or dereferenced, it will add or subtract phrase counts from a collision free map.
    fluoConfig.addObserver(new ObserverConfiguration(DocumentObserver.class.getName()));

    // configure which KryoFactory recipes should use
    KryoSimplerSerializer.setKryoFactory(fluoConfig, PcKryoFactory.class);

    // set up a collision free map to combine phrase counts
    CollisionFreeMap.configure(fluoConfig,
        new CollisionFreeMap.Options(PCM_ID, PhraseMap.PcmCombiner.class,
            PhraseMap.PcmUpdateObserver.class, String.class, Counts.class,
            opts.phraseCountMapBuckets));

    // setup an export queue to to send phrase count updates to an Accumulo table
    ExportQueue.configure(fluoConfig, new ExportQueue.Options(EXPORT_QUEUE_ID, PhraseExporter.class,
        String.class, Counts.class, opts.exportQueueBuckets));
    AccumuloExporter.setExportTableInfo(fluoConfig.getAppConfiguration(), EXPORT_QUEUE_ID,
        new TableInfo(opts.instance, opts.zookeepers, opts.user, opts.password, opts.exportTable));
  }
}
