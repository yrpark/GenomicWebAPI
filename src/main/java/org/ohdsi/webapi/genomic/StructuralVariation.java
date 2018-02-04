package org.ohdsi.webapi.genomic;

import org.ohdsi.webapi.shiro.LogoutFilter;

import java.sql.Timestamp;

/**
 * Created by packyoungjin on 28/09/17.
 */
public class StructuralVariation {
    public Long svDataId;
    public Timestamp updateDatetime;
    public String panel;
    public String cliRrptId;
    public String mutType;
    public String prjId;
    public String secId;
    public String type;
    public Integer gene1ConceptId;
    public String gene1ConceptName;
    public Integer gene2ConceptId;
    public String gene2ConceptName;
    public String mismatches;
    public String strands;
    public String repOverlap;
    public String svType;
    public String readCount;
    public Long nkmers;
    public Long discReadCount;
    public String breakpointCov;
    public String contigId;
    public String contigSeq;
    public String alteration;
    public String alterationDb;
    public String transType;
    public String rearrangementTarget;
    public Integer specimenId;

}
