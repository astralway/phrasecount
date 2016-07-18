package phrasecount;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.recipes.core.types.StringEncoder;
import org.apache.fluo.recipes.core.types.TypeLayer;

public class Constants {

  // set the encoder to use in once place
  public static final TypeLayer TYPEL = new TypeLayer(new StringEncoder());

  public static final Column INDEX_CHECK_COL = TYPEL.bc().fam("index").qual("check").vis();
  public static final Column INDEX_STATUS_COL = TYPEL.bc().fam("index").qual("status").vis();
  public static final Column DOC_CONTENT_COL = TYPEL.bc().fam("doc").qual("content").vis();
  public static final Column DOC_HASH_COL = TYPEL.bc().fam("doc").qual("hash").vis();
  public static final Column DOC_REF_COUNT_COL = TYPEL.bc().fam("doc").qual("refCount").vis();

  public static final String EXPORT_QUEUE_ID = "aeq";
  //phrase count map id
  public static final String PCM_ID = "pcm";
}
