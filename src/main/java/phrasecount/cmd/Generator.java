package phrasecount.cmd;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.ZipfDistributionImpl;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * Generates random documents from a word list using a zipf distribution. This is experimental code.
 * 
 */
public class Generator {

  static class Parameters {
    @Parameter(description = "<word file> <output dir>")
    List<String> args;

    @Parameter(names = {"-s", "--seed"}, description = "Random seed")
    int seed = 13;

    @Parameter(names = {"-n", "--numDocs"}, description = "Number of documents to generate")
    int numDocs = 100;

    @Parameter(names = {"-m", "--maxWords"}, description = "Max number of words to use for generating documents")
    int maxWords = 3000;

    @Parameter(names = {"-l", "--maxWordLen"}, description = "Max word length")
    int maxWordLen = 5;

    @Parameter(names = {"-e", "--exponent"}, description = "Exponent for zipf distribution (non intergers are slower)")
    double exponent = 1.3;

    @Parameter(names = {"-p", "--prefix"}, description = "File name prefix to use")
    String prefix = "rdoc_";

    @Parameter(names = {"-w", "--wordsPerDoc"}, description = "Number of words per doc")
    int wordsPerDoc = 300;

    @Parameter(names = {"-f", "--firstDocId"}, description = "First document number")
    int firstDocument = 0;
  }

  public static void main(String[] args) throws Exception {
    Parameters params = new Parameters();
    JCommander jc = new JCommander(params);

    try {
      jc.parse(args);
      if (params.args == null || params.args.size() != 2)
        throw new ParameterException("Expected 2 arguments");
    } catch (ParameterException pe) {
      System.out.println(pe.getMessage());
      jc.setProgramName(Mini.class.getSimpleName());
      jc.usage();
      System.exit(-1);
    }

    Random rand = new Random(params.seed);

    List<String> words = readWords(params.args.get(0), params.maxWordLen);

    Collections.shuffle(words, rand);
    if (words.size() > params.maxWords)
      words = new ArrayList<String>(words.subList(0, 3000));

    for (int i = 0; i < params.numDocs; i++)
      generateDocument(params, words, rand, i + params.firstDocument);

  }

  private static List<String> readWords(String file, int maxWordLen) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(file));

    ArrayList<String> words = new ArrayList<String>();
    String line;

    while ((line = reader.readLine()) != null)
      if (line.length() <= maxWordLen)
        words.add(line);

    reader.close();

    return words;
  }

  private static void generateDocument(Parameters params, List<String> words, Random rand, int docNum) throws MathException, IOException {
    ZipfDistributionImpl zdi = new ZipfDistributionImpl(words.size(), params.exponent);

    String file = params.args.get(1) + File.separator + params.prefix + String.format("%06d.txt", docNum);
    BufferedWriter writer = new BufferedWriter(new FileWriter(file));

    int len = 0;
    String space = "";
    for (int i = 0; i < params.wordsPerDoc; i++) {
      int wordIndex = zdi.inverseCumulativeProbability(rand.nextDouble());
      String word = words.get(wordIndex);

      writer.write(space);
      writer.write(word);

      len += word.length() + space.length();
      space = " ";

      if (len > 80) {
        writer.write('\n');
        len = 0;
        space = "";
      }

    }

    writer.close();
    System.out.println("wrote : " + file);
  }

}
