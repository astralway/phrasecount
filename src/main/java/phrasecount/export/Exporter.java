package phrasecount.export;


public interface Exporter {
  void export(String phrase, int docCount, int sum, int seqNum) throws Exception;
}
