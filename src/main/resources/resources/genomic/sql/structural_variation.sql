SELECT
sv.SV_DATA_ID,
sv.UPDATE_DATETIME,
sv.PANEL,
sv.CLI_RRPT_ID,
sv.MUT_TYPE,
sv.PRJ_ID,
sv.SEC_ID,
sv.TYPE,
sv.GENE1_CONCEPT_ID,
c1.concept_name as CONCEPT_NAME1,
sv.GENE2_CONCEPT_ID,
c2.concept_name as CONCEPT_NAME2,
sv.MISMATCHES,
sv.STRANDS,
sv.REP_OVERLAP,
sv.SV_TYPE,
sv.READ_COUNT,
sv.NKMERS,
sv.DISC_READ_COUNT,
sv.BREAKPOINT_COV,
sv.CONTIG_ID,
sv.CONTIG_SEQ,
sv.ALTERATION,
sv.ALTERATION_DB,
sv.TRANSTYPE,
sv.REARRANGEMENT_TARGET,
sv.SPECIMEN_ID
FROM @CDM_schema.genomic_structural_variation sv
    INNER JOIN @CDM_schema.concept c1 ON sv.gene1_concept_id =c1.concept_id
    INNER JOIN @CDM_schema.concept c2 ON sv.gene2_concept_id=c2.concept_id
@WHERE_condition

