package org.ohdsi.webapi.genomic;

/**
 * Created by packyoungjin on 28/09/17.
 */

public class Mutation {
    public Long mutation_id;
    public Long omicsMetaId;
    public String chromosome;
    public Long startPosition;
    public Long endPosition;
    public String strand;
    public String variantClassification;
    public String variantType;
    public String referenceAllele;
    public String mutationStatus;
    public String hgvsc;
    public String hgvsp;
    public Long TTotalDepth;
    public Long TRefDepth;
    public Long TVarDepth;
    public Long NTotalDepth;
    public Long NRefDepth;
    public Long NVarDepth;
    public Float alleleFrequency;
    public String exon;
    public Long intron;
    public Long transcriptId;
    public Integer geneConceptId;
    public String geneConceptName;
}
