package phrasecount.cmd;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.LoaderExecutorProperties;

import java.io.File;

import org.apache.commons.io.FileUtils;

import phrasecount.Document;
import phrasecount.DocumentLoader;

public class Load {

  public static void main(String[] args) throws Exception {

    if(args.length != 2){
      System.err.println("Usage : "+Load.class.getName()+" <fluo props file> <txt file dir>");
      System.exit(-1);
    }
    
    LoaderExecutorProperties leprops = new LoaderExecutorProperties(new File(args[0]));
    leprops.setNumThreads(20);
    leprops.setQueueSize(40);

    try (FluoClient fluoClient = FluoFactory.newClient(leprops); LoaderExecutor le = fluoClient.newLoaderExecutor()) {
      for (File txtFile : FileUtils.listFiles(new File(args[1]), new String[] {"txt"}, true)) {
        String uri = txtFile.toURI().toString();
        String content = FileUtils.readFileToString(txtFile);

        System.out.println("Processing : " + txtFile.toURI());
        le.execute(new DocumentLoader(new Document(uri, content)));
      }
    }

    // TODO figure what threads are hanging around
    System.exit(0);
  }

}
