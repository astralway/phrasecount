package phrasecount.pojos;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import phrasecount.PhraseExport;

public class PcKryoFactory implements KryoFactory {
  @Override
  public Kryo create() {
    Kryo kryo = new Kryo();
    kryo.register(Counts.class, 9);
    kryo.register(PhraseExport.class, 10);
    return kryo;
  }
}
