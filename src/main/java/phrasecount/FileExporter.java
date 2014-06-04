package phrasecount;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Exports phrase counts to a file. Efficiently allows multiple threads to concurrently export phrases.
 */

public class FileExporter {

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

  private static LinkedBlockingQueue<ExportItem> exportQueue = null;

  private static class ExportItem {
    String exportData;
    CountDownLatch cdl = new CountDownLatch(1);

    ExportItem(String data) {
      this.exportData = data;
    }
  }

  void export(String phrase, int docCount, int sum, int seqNum) throws InterruptedException, IOException {
    ExportItem expItem = new ExportItem(docCount + ":" + sum + ":" + seqNum + ":" + phrase);
    // Do not want each transaction to open, write, and close the resource, better to batch the writes of many threads into a single write.
    // If there is too much in the queue, then this will throw an exception... that will fail the transaction and cause it to retry later
    exportQueue.add(expItem);

    // Wait for data to be written. If this was not done then the transaction could finish and the process die before the data was written. In this case the
    // export data would be lost.
    expItem.cdl.await();
  }

  private FileExporter(String file) throws IOException {
    exportQueue = new LinkedBlockingQueue<ExportItem>(10000);
    Thread queueProcessingTask = new Thread(new ExportTask(file));

    queueProcessingTask.setDaemon(true);
    queueProcessingTask.start();
  }

  private static Map<String,FileExporter> exporters = new HashMap<String,FileExporter>();

  static synchronized FileExporter getInstance(String file) throws IOException {

    FileExporter ret = exporters.get(file);

    if (ret == null) {
      ret = new FileExporter(file);
      exporters.put(file, ret);
    }

    return ret;
  }
}
