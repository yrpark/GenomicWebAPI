package org.ohdsi.webapi.genomic;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by packyoungjin on 21/12/17.
 */
public class GenomicOmicsMeta {
    public Long omicsMetaId;
    public Long specimenId;
    public String sourceType;
    public String assayType;
    public String panelName;
    public String enrichmentMethods;
    public String instrument;
    public String center;
    public String sequenceReadLength;
    public String sequenceLayer;
    public String referenceGenomeBuild;
    public String analysisTool;
    public String geneAnnotationDb;
    public Integer tumorCellularity;

    public List<CopyNumberVariation> copyNumberVariations;
    public List<Mutation> mutations;
    public List<StructuralVariation> structuralVariations;
    public List<Expression> expressions;

    public GenomicOmicsMeta() {
        copyNumberVariations = new ArrayList<>();
        mutations = new ArrayList<>();
        structuralVariations = new ArrayList<>();
        expressions = new ArrayList<>();
    }
}
