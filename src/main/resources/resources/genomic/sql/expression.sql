SELECT
expression.EXPRESSION_ID,
expression.OMICS_META_ID,
expression.GENE_CONCEPT_ID,
c1.CONCEPT_NAME,
expression.EXPRESSION_VALUE
FROM @CDM_schema.genomic_expression expression
    INNER JOIN @CDM_schema.concept c1 ON expression.gene_concept_id =c1.concept_id
@WHERE_condition
