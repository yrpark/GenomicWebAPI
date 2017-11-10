package org.ohdsi.webapi.genomic;

import java.sql.Timestamp;

/**
 * Created by packyoungjin on 28/09/17.
 */
public class CopyNumberVariation {
    public Long cnvDataId;
    public Timestamp updateDatetime;
    public String panel;
    public String cliRrptId;
    public String mutType;
    public String smpId;
    public String prjId;
    public String secId;
    public String type;
    public String chromosome;
    public Long start;
    public Long end;
    public Integer geneConceptId;
    public Float log2;
    public Integer cn;
    public String alteration;
    public String alterationDb;
    public String sigflag;
    public String hrd;
    public String cnvType;
    public String knownFlag;
    public Integer specimenId;
}
