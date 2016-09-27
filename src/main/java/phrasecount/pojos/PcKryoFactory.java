package phrasecount.pojos;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;

public class PcKryoFactory implements KryoFactory {
  @Override
  public Kryo create() {
    Kryo kryo = new Kryo();
    kryo.register(Counts.class, 9);
    return kryo;
  }
}
