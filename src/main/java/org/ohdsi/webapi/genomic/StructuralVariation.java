package org.ohdsi.webapi.genomic;

import org.ohdsi.webapi.shiro.LogoutFilter;

import java.sql.Timestamp;

/**
 * Created by packyoungjin on 28/09/17.
 */
public class StructuralVariation {
    public Long svId;
    public Long omicsMetaId;
    public Integer gene1ConceptId;
    public String gene1ConceptName;
    public Integer gene2ConceptId;
    public String gene2ConceptName;
    public Integer gene3ConceptId;
    public String gene3ConceptName;
    public String breakpoint1;
    public String breakpoint2;
    public String breakpoint3;
    public String strands1;
    public String strands2;
    public String strands3;
    public String supportingReads1;
    public String supportingReads2;
    public String supportingReads3;
    public String contigSequence;
}
