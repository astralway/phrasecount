package phrasecount.cmd;

import java.io.File;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.config.FluoConfiguration;
import phrasecount.DocumentLoader;
import phrasecount.pojos.Document;

public class Load {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("Usage : " + Load.class.getName() + " <fluo props file> <txt file dir>");
      System.exit(-1);
    }

    FluoConfiguration config = new FluoConfiguration(new File(args[0]));
    config.setLoaderThreads(20);
    config.setLoaderQueueSize(40);

    try (FluoClient fluoClient = FluoFactory.newClient(config);
        LoaderExecutor le = fluoClient.newLoaderExecutor()) {
      File[] files = new File(args[1]).listFiles();

      if (files == null) {
        System.out.println("Text file dir does not exist: " + args[1]);
      } else {
        for (File txtFile : files) {
          if (txtFile.getName().endsWith(".txt")) {
            String uri = txtFile.toURI().toString();
            String content = Files.toString(txtFile, Charsets.UTF_8);

            System.out.println("Processing : " + txtFile.toURI());
            le.execute(new DocumentLoader(new Document(uri, content)));
          } else {
            System.out.println("Ignoring : " + txtFile.toURI());
          }
        }
      }
    }

    // TODO figure what threads are hanging around
    System.exit(0);
  }
}
