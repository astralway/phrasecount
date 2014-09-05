package phrasecount;

import io.fluo.api.data.Column;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;

public class Constants {
  // set the encoder to use in once place
  public static final TypeLayer TYPEL = new TypeLayer(new StringEncoder());

  public static final Column INDEX_CHECK_COL = TYPEL.bc().fam("index").qual("check").vis();
  public static final Column INDEX_STATUS_COL = TYPEL.bc().fam("index").qual("status").vis();
  public static final Column DOC_CONTENT_COL = TYPEL.bc().fam("doc").qual("content").vis();
  public static final Column DOC_HASH_COL = TYPEL.bc().fam("doc").qual("hash").vis();
  public static final Column DOC_REF_COUNT_COL = TYPEL.bc().fam("doc").qual("refCount").vis();
  public static final Column STAT_CHECK_COL = TYPEL.bc().fam("stat").qual("check").vis();
  public static final Column STAT_DOC_COUNT_COL = TYPEL.bc().fam("stat").qual("docCount").vis();
  public static final Column STAT_SUM_COL = TYPEL.bc().fam("stat").qual("sum").vis();
  public static final Column EXPORT_CHECK_COL = TYPEL.bc().fam("export").qual("check").vis();
  public static final Column EXPORT_DOC_COUNT_COL = TYPEL.bc().fam("export").qual("docCount").vis();
  public static final Column EXPORT_SEQ_COL = TYPEL.bc().fam("export").qual("seq").vis();
  public static final Column EXPORT_SUM_COL = TYPEL.bc().fam("export").qual("sum").vis();
}
