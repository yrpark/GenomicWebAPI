SELECT
cnv.CNV_DATA_ID,
cnv.UPDATE_DATETIME,
cnv.PANEL,
cnv.CLI_RRPT_ID,
cnv.MUT_TYPE,
cnv.PRJ_ID,
cnv.SEC_ID,
cnv.TYPE,
cnv.START AS ST,
cnv.END AS EN,
cnv.CHROMOSOME,
cnv.GENE_CONCEPT_ID,
c1.CONCEPT_NAME,
cnv.LOG2,
cnv.CN,
cnv.ALTERATION,
cnv.ALTERATION_DB,
cnv.SIGFLAG,
cnv.HRD,
cnv.CNVTYPE,
cnv.KNOWNFLAG,
cnv.SPECIMEN_ID
FROM @CDM_schema.genomic_copy_number_variation cnv
    INNER JOIN @CDM_schema.concept c1 ON cnv.gene_concept_id =c1.concept_id
@WHERE_condition
