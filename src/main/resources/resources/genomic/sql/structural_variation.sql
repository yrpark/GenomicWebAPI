SELECT
sv.SV_ID,
sv.OMICS_META_ID,
sv.GENE1_CONCEPT_ID,
c1.concept_name as CONCEPT_NAME1,
sv.GENE2_CONCEPT_ID,
c2.concept_name as CONCEPT_NAME2,
sv.GENE3_CONCEPT_ID,
c3.concept_name as CONCEPT_NAME3,
sv.BREAKPOINT1,
sv.BREAKPOINT2,
sv.BREAKPOINT3,
sv.STRANDS1,
sv.STRANDS2,
sv.STRANDS3,
sv.SUPPORTING_READS1,
sv.SUPPORTING_READS2,
sv.SUPPORTING_READS3,
sv.CONTIG_SEQUENCE
FROM @CDM_schema.genomic_sv sv
    INNER JOIN @CDM_schema.concept c1 ON sv.gene1_concept_id =c1.concept_id
    INNER JOIN @CDM_schema.concept c2 ON sv.gene2_concept_id=c2.concept_id
    INNER JOIN @CDM_schema.concept c3 ON sv.gene3_concept_id=c2.concept_id
@WHERE_condition

