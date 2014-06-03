package phrasecount;

import static phrasecount.Constants.EXPORT_CHECK_COL;
import static phrasecount.Constants.EXPORT_DOC_COUNT_COL;
import static phrasecount.Constants.EXPORT_SEQ_COL;
import static phrasecount.Constants.EXPORT_SUM_COL;
import static phrasecount.Constants.STAT_DOC_COUNT_COL;
import static phrasecount.Constants.STAT_SUM_COL;
import static phrasecount.Constants.TYPEL;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.data.ByteSequence;

import accismus.api.Column;
import accismus.api.Observer;
import accismus.api.Transaction;
import accismus.api.types.TypedSnapshot.Value;
import accismus.api.types.TypedTransaction;

import com.google.common.collect.Sets;

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
public class PhraseExporter implements Observer {


  public void process(Transaction tx, ByteSequence row, Column col) throws Exception {
    TypedTransaction ttx = TYPEL.transaction(tx);
    
    Map<Column,Value> columns = ttx.getd(row, Sets.newHashSet(STAT_SUM_COL, STAT_DOC_COUNT_COL, EXPORT_SUM_COL, EXPORT_DOC_COUNT_COL, EXPORT_SEQ_COL));
    
    Integer currentSum = columns.get(STAT_SUM_COL).toInteger();
    Integer currentDocCount = columns.get(STAT_DOC_COUNT_COL).toInteger();

    int exportSum = columns.get(EXPORT_SUM_COL).toInteger();
    int exportDocCount = columns.get(EXPORT_DOC_COUNT_COL).toInteger();

    // TODO its possible that the Accismus column times stamp could be used as a seq number
    int seqNum = columns.get(EXPORT_SEQ_COL).toInteger(0);
    
    export(row.toString().substring("phrase:".length()), exportDocCount, exportSum, seqNum);
    
    // check to see if the current value has changed and another export is needed
    if (currentSum != null && !currentSum.equals(exportSum)) {
      // initiate another export transaction
      ttx.set().row(row).col(EXPORT_SUM_COL).val(currentSum);
      ttx.set().row(row).col(EXPORT_DOC_COUNT_COL).val(currentDocCount);
      ttx.set().row(row).col(EXPORT_CHECK_COL).val();
    } else {
      ttx.delete(row, EXPORT_SUM_COL);
      ttx.delete(row, EXPORT_DOC_COUNT_COL);
      ttx.delete(row, EXPORT_CHECK_COL);
    }

    ttx.set().row(row).col(EXPORT_SEQ_COL).val(seqNum + 1);
  }

  private static void export(String phrase, int docCount, int sum, int seqNum) throws InterruptedException, IOException {
    // TODO need a way to configure Observers, maybe an initialize method that takes config
    startFileWriterTask("phrase_export.txt");


    ExportItem expItem = new ExportItem(docCount + ":" + sum + ":" + seqNum + ":" + phrase);
    // Do not want each transaction to open, write, and close the resource, better to batch the writes of many threads into a single write.
    // If there is too much in the queue, then this will throw an exception... that will fail the transaction and cause it to retry later
    exportQueue.add(expItem);

    // Wait for data to be written. If this was not done then the transaction could finish and the process die before the data was written. In this case the
    // export data would be lost.
    expItem.cdl.await();
  }

  private static LinkedBlockingQueue<ExportItem> exportQueue = null;

  private static class ExportItem {
    String exportData;
    CountDownLatch cdl = new CountDownLatch(1);

    ExportItem(String data) {
      this.exportData = data;
    }
  }

  private static class ExportTask implements Runnable {

    Writer out;

    ExportTask(String file) throws IOException {
      out = new BufferedWriter(new FileWriter(file));
    }

    public void run() {

      ArrayList<ExportItem> exports = new ArrayList<ExportItem>();

      while (true) {
        try {
          exports.clear();

          // gather export from all threads that have placed an item on the queue
          exports.add(exportQueue.take());
          exportQueue.drainTo(exports);

          for (ExportItem ei : exports) {
            out.write(ei.exportData);
            out.write('\n');
          }

          // TODO could fsync
          out.flush();

          // notify all threads waiting after flushing
          for (ExportItem ei : exports)
            ei.cdl.countDown();

        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

  }

  private static synchronized void startFileWriterTask(String file) throws IOException {
    if (exportQueue == null) {
      exportQueue = new LinkedBlockingQueue<ExportItem>(10000);
      Thread queueProcessingTask = new Thread(new ExportTask(file));

      queueProcessingTask.setDaemon(true);
      queueProcessingTask.start();
    }
  }

}
