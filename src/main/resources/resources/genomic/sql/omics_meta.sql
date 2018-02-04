select
OMICS_META_ID,
SPECIMEN_ID,
ASSAY_TYPE,
SOURCE_TYPE,
PANEL_NAME,
ENRICHMENT_METHODS,
INSTRUMENT,
CENTER,
SEQUENCE_READ_LENGTH,
SEQUENCING_LAYER,
REFERENCE_GENOME_BUILD,
ANALYSIS_TOOL,
GENE_ANNOTATION_DB,
TUMOR_CELLULARITY
FROM  @CDM_schema.genomic_omics_meta
@WHERE_condition