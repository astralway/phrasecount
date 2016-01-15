package phrasecount.pojos;

import java.util.HashMap;
import java.util.Map;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class Document {
  // the location where the document came from. This is needed inorder to detect when a document
  // changes.
  private String uri;

  // the text of a document.
  private String content;

  private String hash = null;

  public Document(String uri, String content) {
    this.content = content;
    this.uri = uri;
  }

  public String getURI() {
    return uri;
  }

  public String getHash() {
    if (hash != null)
      return hash;

    Hasher hasher = Hashing.sha1().newHasher();
    String[] tokens = content.toLowerCase().split("[^\\p{Alnum}]+");

    for (String token : tokens) {
      hasher.putString(token);
    }

    return hash = hasher.hash().toString();
  }

  public Map<String, Integer> getPhrases() {
    String[] tokens = content.toLowerCase().split("[^\\p{Alnum}]+");

    Map<String, Integer> phrases = new HashMap<>();
    for (int i = 3; i < tokens.length; i++) {
      String phrase = tokens[i - 3] + " " + tokens[i - 2] + " " + tokens[i - 1] + " " + tokens[i];
      Integer old = phrases.put(phrase, 1);
      if (old != null)
        phrases.put(phrase, 1 + old);
    }

    return phrases;
  }

  public String getContent() {
    return content;
  }
}
