SELECT
cnv.CNV_ID,
cnv.OMICS_META_ID,
cnv.CHROMOSOME,
cnv.GENE_CONCEPT_ID,
c1.CONCEPT_NAME,
cnv.LOG2_RATIO,
cnv.ESTIMATED_CN,
cnv.SEGMENT_START,
cnv.SEGMENT_END
FROM @CDM_schema.genomic_cnv cnv
    INNER JOIN @CDM_schema.concept c1 ON cnv.gene_concept_id =c1.concept_id
@WHERE_condition
