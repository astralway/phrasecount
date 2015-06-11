package phrasecount;

import java.util.Map;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.api.types.TypedSnapshotBase.Value;
import io.fluo.api.types.TypedTransactionBase;
import phrasecount.export.AccumuloExporter;
import phrasecount.export.Exporter;
import phrasecount.export.FileExporter;

import static phrasecount.Constants.EXPORT_CHECK_COL;
import static phrasecount.Constants.EXPORT_DOC_COUNT_COL;
import static phrasecount.Constants.EXPORT_SEQ_COL;
import static phrasecount.Constants.EXPORT_SUM_COL;
import static phrasecount.Constants.STAT_DOC_COUNT_COL;
import static phrasecount.Constants.STAT_SUM_COL;
import static phrasecount.Constants.TYPEL;

/**
 * PhraseExporter exports phrase counts to an external source. The following problems must be considered when exporting data.
 *
 * <UL>
 * <LI>Exports arriving out of order. This could be caused by one thread reading the export columns and then pausing for a long period of time. When the thread
 * unpauses and exports the data, the data could be old/stale. Combine this other threads attempting to export the data and this could cause older data to
 * arrive after newer data.
 * <LI>Export transactions failing before actually exporting data. Multiple attempts to export data may fail at different points multiple times before
 * successfully exporting data.
 * </UL>
 *
 * To ensure that concurrency and faults do not cause problem, an export transaction will only export columns committed in a previous transaction. Also a count
 * will be set which can be used to order data. The general idea is the following :
 *
 * <UL>
 * <LI>transaction 1 commits the export columns
 * <LI>transaction 2 reads the export columns and writes them externally with the count AND increments the count
 * </UL>
 *
 * The export function should be idempotent, it should be ok to run it multiple times w/ the same inputs.
 *
 * If the phrase counts have changed while the export was in progress, then this observer will trigger itself.
 *
 */
public class PhraseExporter extends AbstractObserver {

  private Exporter exporter;

  private String get(Map<String,String> config, String key, String defaultValue) {
    String val = config.get(key);
    if (val == null)
      return defaultValue;
    return val;
  }

  @Override
  public void init(Context context) throws Exception {
     Map<String, String> config = context.getParameters();
    String sink = get(config, "sink", "file");
    if (sink.equals("file")) {
      String file = get(config, "file", "phrase_export.txt");
      exporter = FileExporter.getInstance(file);
    } else if (sink.equals("accumulo")) {
      String instance = config.get("instance");
      String zookeepers = config.get("zookeepers");
      String user = config.get("user");
      String password = config.get("password");
      String table = config.get("table");
      exporter = AccumuloExporter.getInstance(instance, zookeepers, user, password, table);
    }
  }

  // TODO this needs test
  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    TypedTransactionBase ttx = TYPEL.wrap(tx);

    Map<Column,Value> columns = ttx.get().row(row).columns(STAT_SUM_COL, STAT_DOC_COUNT_COL, EXPORT_SUM_COL, EXPORT_DOC_COUNT_COL, EXPORT_SEQ_COL);

    Integer currentSum = columns.get(STAT_SUM_COL).toInteger();
    Integer currentDocCount = columns.get(STAT_DOC_COUNT_COL).toInteger();

    Integer exportSum = columns.get(EXPORT_SUM_COL).toInteger();
    Integer exportDocCount = columns.get(EXPORT_DOC_COUNT_COL).toInteger();

    if (exportSum == null) {
      return;
    }

    // TODO its possible that the Fluo column times stamp could be used as a seq number
    int seqNum = columns.get(EXPORT_SEQ_COL).toInteger(0);

    // check to see if the current value has changed and another export is needed
    if (currentSum != null && !currentSum.equals(exportSum)) {
      // initiate another export transaction
      ttx.mutate().row(row).col(EXPORT_SUM_COL).set(currentSum);
      ttx.mutate().row(row).col(EXPORT_DOC_COUNT_COL).set(currentDocCount);
      ttx.mutate().row(row).col(EXPORT_CHECK_COL).set();
    } else {
      exporter.export(row.toString().substring("phrase:".length()), exportDocCount, exportSum, seqNum);

      ttx.delete(row, EXPORT_SUM_COL);
      ttx.delete(row, EXPORT_DOC_COUNT_COL);
      // TODO modifying trigger is broken
      // ttx.delete(row, EXPORT_CHECK_COL);
    }

    ttx.mutate().row(row).col(EXPORT_SEQ_COL).set(seqNum + 1);
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(EXPORT_CHECK_COL, NotificationType.STRONG);
  }
}
