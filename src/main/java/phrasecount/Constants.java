package phrasecount;

import accismus.api.Column;
import accismus.api.types.StringEncoder;
import accismus.api.types.TypeLayer;

public class Constants {
  // set the encoder to use in once place
  public static final TypeLayer TYPEL = new TypeLayer(new StringEncoder());

  public static final Column INDEX_CHECK_COL = TYPEL.newColumn("index", "check");
  public static final Column INDEX_STATUS_COL = TYPEL.newColumn("index", "status");
  public static final Column DOC_CONTENT_COL = TYPEL.newColumn("doc", "content");
  public static final Column DOC_HASH_COL = TYPEL.newColumn("doc", "hash");
  public static final Column DOC_REF_COUNT_COL = TYPEL.newColumn("doc", "refCount");
  public static final Column STAT_DOC_COUNT_COL = TYPEL.newColumn("stat", "docCount");
  public static final Column STAT_SUM_COL = TYPEL.newColumn("stat", "sum");
  public static final Column EXPORT_CHECK_COL = TYPEL.newColumn("export", "check");
  public static final Column EXPORT_DOC_COUNT_COL = TYPEL.newColumn("export", "docCount");
  public static final Column EXPORT_SEQ_COL = TYPEL.newColumn("export", "seq");
  public static final Column EXPORT_SUM_COL = TYPEL.newColumn("export", "sum");
}
