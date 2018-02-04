package org.ohdsi.webapi.genomic;

import java.sql.Timestamp;

/**
 * Created by packyoungjin on 28/09/17.
 */
public class CopyNumberVariation {
    public Long cnvId;
    public Long omicsMetaId;
    public String chromosome;
    public Integer geneConceptId;
    public String geneConceptName;
    public Float log2Ratio;
    public Integer estimatedCn;
    public Long segmentStart;
    public Long segmentEnd;
}
