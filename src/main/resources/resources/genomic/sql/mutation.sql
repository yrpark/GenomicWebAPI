select
mutation.MUTATION_ID,
mutation.OMICS_META_ID,
mutation.CHROMOSOME,
mutation.START_POSITION,
mutation.END_POSITION,
mutation.STRAND,
mutation.VARIANT_CLASSIFICATION,
mutation.VARIANT_TYPE,
mutation.REFERENCE_ALLELE,
mutation.MUTATION_STATUS,
mutation.HGVSC,
mutation.HGVSP,
mutation.T_TOTAL_DEPTH,
mutation.T_REF_DEPTH,
mutation.T_VAR_DEPTH,
mutation.N_TOTAL_DEPTH,
mutation.N_REF_DEPTH,
mutation.N_VAR_DEPTH,
mutation.ALLELE_FREQUENCY,
mutation.EXON,
mutation.INTRON,
mutation.TRANSCRIPT_ID,
mutation.GENE_CONCEPT_ID,
c1.CONCEPT_NAME
FROM @CDM_schema.genomic_mutation mutation
    INNER JOIN @CDM_schema.concept c1 ON mutation.gene_concept_id =c1.concept_id
@WHERE_condition
