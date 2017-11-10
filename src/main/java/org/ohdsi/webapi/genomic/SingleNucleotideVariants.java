package org.ohdsi.webapi.genomic;

import org.ohdsi.webapi.person.PersonRecord;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Created by packyoungjin on 28/09/17.
 */

public class SingleNucleotideVariants {
    public Long snvDataId;
    public Timestamp updateDatetime;
    public String panel;
    public String cliRrptId;
    public String mutType;
    public String smpId;
    public String prjId;
    public String secId;
    public Integer geneConceptId;
    public String ensId;
    public String mutationStatus;
    public String chromosome;
    public Long start;
    public Long end;
    public String ref;
    public String var;
    public String variantClass;
    public String variantType;
    public String hgvsc;
    public String hgvsp;
    public String hgvspDb;
    public String dbsnp;
    public Long TTotalDepth;
    public Long TRefDepth;
    public Long TVarDepth;
    public Long NTotalDepth;
    public Long NRefDepth;
    public Long NVarDepth;
    public Float alleleFreq;
    public String strand;
    public String eoxn;
    public Long intron;
    public String sift;
    public String polyphen;
    public String domain;
    public String hrd;
    public String mmr;
    public String zygosity;
    public Integer transcriptrank;
    public String diagnosis;
    public String drug;
    public String drugable;
    public String whitelist;
    public Integer specimenId;
}
