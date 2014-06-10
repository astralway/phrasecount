package phrasecount.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;

public class AccumuloExporter implements Exporter {

  private static class ExportTask implements Runnable {

    private BatchWriter bw;

    public ExportTask(String instanceName, String zookeepers, String user, String password, String table) throws TableNotFoundException, AccumuloException,
        AccumuloSecurityException {
      ZooKeeperInstance zki = new ZooKeeperInstance(instanceName, zookeepers);

      // TODO need to close batch writer
      bw = zki.getConnector(user, new PasswordToken(password)).createBatchWriter(table, new BatchWriterConfig());
    }

    @Override
    public void run() {

      ArrayList<ExportItem> exports = new ArrayList<ExportItem>();

      while (true) {
        try {
          exports.clear();

          // gather export from all threads that have placed an item on the queue
          exports.add(exportQueue.take());
          exportQueue.drainTo(exports);

          for (ExportItem ei : exports) {
            Mutation mutation = new Mutation(ei.phrase);

            // use the sequence number for the Accumulo timestamp, this will cause older updates to fall behind newer ones
            mutation.put("stat", "sum", ei.seqNum, ei.sum + "");
            mutation.put("stat", "docCount", ei.seqNum, ei.docCount + "");

            bw.addMutation(mutation);
          }

          bw.flush();

          // notify all threads waiting after flushing
          for (ExportItem ei : exports)
            ei.cdl.countDown();

        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (MutationsRejectedException e) {
          throw new RuntimeException(e);
        }
      }
    }

  }

  private static LinkedBlockingQueue<ExportItem> exportQueue = null;

  private static class ExportItem {
    String phrase;
    int docCount;
    int sum;
    int seqNum;

    CountDownLatch cdl = new CountDownLatch(1);

    ExportItem(String phrase, int docCount, int sum, int seqNum) {
      this.phrase = phrase;
      this.docCount = docCount;
      this.sum = sum;
      this.seqNum = seqNum;
    }
  }

  public AccumuloExporter(String instanceName, String zookeepers, String user, String password, String table) throws Exception,
      AccumuloSecurityException {

    exportQueue = new LinkedBlockingQueue<ExportItem>(10000);
    Thread queueProcessingTask = new Thread(new ExportTask(instanceName, zookeepers, user, password, table));

    queueProcessingTask.setDaemon(true);
    queueProcessingTask.start();
  }

  @Override
  public void export(String phrase, int docCount, int sum, int seqNum) throws Exception {

    ExportItem ei = new ExportItem(phrase, docCount, sum, seqNum);

    exportQueue.add(ei);

    ei.cdl.await();

  }

  private static Map<String,AccumuloExporter> exporters = new HashMap<String,AccumuloExporter>();

  public static synchronized AccumuloExporter getInstance(String instanceName, String zookeepers, String user, String password, String table) throws Exception {

    String key = instanceName + ":" + zookeepers + ":" + user + ":" + password.hashCode() + ":" + table;

    AccumuloExporter ret = exporters.get(key);

    if (ret == null) {
      ret = new AccumuloExporter(instanceName, zookeepers, user, password, table);
      exporters.put(key, ret);
    }

    return ret;
  }

}
