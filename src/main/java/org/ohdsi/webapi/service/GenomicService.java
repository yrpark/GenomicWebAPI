package org.ohdsi.webapi.service;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.eclipse.collections.impl.list.mutable.ListAdapter;
import org.ohdsi.circe.helper.ResourceHelper;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.genomic.*;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;


@Path("/genomic/{sourceKey}")
@Component
public class GenomicService extends AbstractDaoService {

    public enum OmicsMetaQueryType {
        SPECIMEN, //SPECIMEN 정보만
        OMICS  //OMICS 정보포함
    }


    public enum SPECIMEN_QUERYTYPE {
        SPECIMEN_ID, //SPECIMEN
        PERSON_ID,  // person 정보
        OMICSMETA_ID
    }

    public GenomicPerson getGenomicPerson(String sourceKey, String personID){
        GenomicPerson person = new GenomicPerson() ;

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/person.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "PERSON_ID"}, new String[]{tableQualifier, personID});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
        log.info("Query " + sql_statement);

        getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
            @Override
            public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                person.personId = rs.getLong("PERSON_ID");
                person.gender = rs.getString("GENDER_SOURCE_VALUE");
                person.yearOfBirth = rs.getInt("YEAR_OF_BIRTH");
                person.monthOfBirth = rs.getInt("MONTH_OF_BIRTH");
                person.dayOfBirth = rs.getInt("DAY_OF_BIRTH");
                person.deathDate = rs.getDate("DEATH_DATE");
                return null;
            }
        });


        sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/measurment.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "PERSON_ID"}, new String[]{tableQualifier,personID});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
        log.info("Query " + sql_statement);

        getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
            @Override
            public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                Measurement measurement = new Measurement();

                measurement.measurementConceptID = rs.getInt("MEASUREMENT_CONCEPT_ID");
                measurement.measurementDateTime = rs.getTimestamp("MEASUREMENT_DATE");
                //
                //measurement.measurementDateTime = rs.getTimestamp("MEASUREMENT_DATETIME");
                measurement.valueAsNumber = rs.getInt("VALUE_AS_NUMBER");
                person.measurement.add(measurement);
                return null;
            }
        });


        sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/procedureoccurrence.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "PERSON_ID"}, new String[]{tableQualifier,personID});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
        log.info("Query " + sql_statement);

        getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
            @Override
            public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                ProcedureOccurrence occurrence = new ProcedureOccurrence();

                occurrence.procedureConceptId = rs.getInt("PROCEDURE_CONCEPT_ID");
                //occurrence.procedureDateTime = rs.getTimestamp("PROCEDURE_DATETIME");;
                occurrence.procedureDateTime = rs.getTimestamp("PROCEDURE_DATE");
                person.procedureoccurrence.add(occurrence);
                return null;
            }
        });

        return person;
    }


    private final RowMapper<CopyNumberVariation> cnvMapper = new RowMapper<CopyNumberVariation>() {
        @Override
        public CopyNumberVariation mapRow(ResultSet rs, int rowNum) throws SQLException {
            CopyNumberVariation report = new CopyNumberVariation();

            report.cnvId = rs.getLong("CNV_ID");
            report.omicsMetaId = rs.getLong("OMICS_META_ID");
            report.chromosome = rs.getString("CHROMOSOME");
            report.geneConceptId = rs.getInt("GENE_CONCEPT_ID");
            report.geneConceptName = rs.getString("CONCEPT_NAME");
            report.log2Ratio = rs.getFloat("log2_ratio");
            report.estimatedCn = rs.getInt("estimated_cn");
            report.segmentStart = rs.getLong("segment_start");
            report.segmentEnd = rs.getLong("segment_end");

            return report;
        }
    };


    private final RowMapper<Mutation> mutationMapper = new RowMapper<Mutation>() {
        @Override
        public Mutation mapRow(ResultSet rs, int rowNum) throws SQLException {
            Mutation report = new Mutation();
            report.mutation_id = rs.getLong("MUTATION_ID");
            report.omicsMetaId = rs.getLong("OMICS_META_ID");
            report.chromosome = rs.getString("CHROMOSOME");
            report.startPosition = rs.getLong("START_POSITION");
            report.endPosition = rs.getLong("END_POSITION");
            report.strand = rs.getString("STRAND");
            report.variantClassification = rs.getString("VARIANT_CLASSIFICATION");
            report.variantType = rs.getString("VARIANT_TYPE");
            report.referenceAllele = rs.getString("REFERENCE_ALLELE");
            report.mutationStatus = rs.getString("MUTATION_STATUS");
            report.hgvsc = rs.getString("HGVSC");
            report.hgvsp = rs.getString("HGVSP");
            report.TTotalDepth = rs.getLong("T_TOTAL_DEPTH");
            report.TRefDepth = rs.getLong("T_REF_DEPTH");
            report.TVarDepth = rs.getLong("T_VAR_DEPTH");
            report.NTotalDepth = rs.getLong("N_TOTAL_DEPTH");
            report.NRefDepth = rs.getLong("N_REF_DEPTH");
            report.NVarDepth = rs.getLong("N_VAR_DEPTH");
            report.alleleFrequency = rs.getFloat("ALLELE_FREQUENCY");
            report.exon = rs.getString("EXON");
            report.intron = rs.getLong("INTRON");
            report.transcriptId = rs.getLong("TRANSCRIPT_ID");
            report.geneConceptId = rs.getInt("GENE_CONCEPT_ID");
            report.geneConceptName = rs.getString("CONCEPT_NAME");

            return report;
        }
    };

    private final RowMapper<StructuralVariation> svMapper = new RowMapper<StructuralVariation>() {
        @Override
        public StructuralVariation mapRow(ResultSet rs, int rowNum) throws SQLException {
            StructuralVariation report = new StructuralVariation();
            report.svId = rs.getLong("SV_ID");
            report.omicsMetaId = rs.getLong("OMICS_META_ID");
            report.gene1ConceptId = rs.getInt("GENE1_CONCEPT_ID");
            report.gene1ConceptName = rs.getString("CONCEPT_NAME1");
            report.gene2ConceptId = rs.getInt("GENE2_CONCEPT_ID");
            report.gene2ConceptName = rs.getString("CONCEPT_NAME2");
            report.gene3ConceptId = rs.getInt("GENE2_CONCEPT_ID");
            report.gene3ConceptName = rs.getString("CONCEPT_NAME3");
            report.breakpoint1 = rs.getString("CONCEPT_NAME2");
            report.breakpoint2 = rs.getString("CONCEPT_NAME2");
            report.breakpoint3 = rs.getString("CONCEPT_NAME3");
            report.strands1 = rs.getString("STRANDS1");
            report.strands2 = rs.getString("STRANDS2");
            report.strands3 = rs.getString("STRANDS3");
            report.supportingReads1 = rs.getString("SUPPORTING_READS1");
            report.supportingReads2 = rs.getString("SUPPORTING_READS2");
            report.supportingReads3 = rs.getString("SUPPORTING_READS3");
            report.contigSequence = rs.getString("CONTIG_SEQUENCE");
            return report;
        }
    };

    private final RowMapper<Expression> expressionMapper = new RowMapper<Expression>() {
        @Override
        public Expression mapRow(ResultSet rs, int rowNum) throws SQLException {
            Expression report = new Expression();
            report.expressionId = rs.getLong("EXPRESSION_ID");
            report.omicsMetaId = rs.getLong("OMICS_META_ID");
            report.geneConceptId = rs.getLong("GENE_CONCEPT_ID");
            report.geneConceptName = rs.getString("CONCEPT_NAME");
            return report;
        }
    };

    private final RowMapper<GenomicOmicsMeta> omicsMetaRowMapper = new RowMapper<GenomicOmicsMeta>() {
        @Override
        public GenomicOmicsMeta mapRow(ResultSet rs, int rowNum) throws SQLException {
            GenomicOmicsMeta report = new GenomicOmicsMeta();

            report.omicsMetaId = rs.getLong("OMICS_META_ID");
            report.specimenId = rs.getLong("SPECIMEN_ID");
            report.sourceType = rs.getString("SOURCE_TYPE");
            report.panelName = rs.getString("PANEL_NAME");
            report.assayType = rs.getString("ASSAY_TYPE");
            report.enrichmentMethods = rs.getString("ENRICHMENT_METHODS");
            report.instrument = rs.getString("INSTRUMENT");
            report.center = rs.getString("CENTER");
            report.sequenceReadLength = rs.getString("SEQUENCE_READ_LENGTH");
            report.sequenceLayer = rs.getString("SEQUENCING_LAYER");
            report.referenceGenomeBuild = rs.getString("REFERENCE_GENOME_BUILD");
            report.analysisTool = rs.getString("ANALYSIS_TOOL");
            report.geneAnnotationDb = rs.getString("GENE_ANNOTATION_DB");
            report.tumorCellularity = rs.getInt("TUMOR_CELLULARITY");

            return report;
        }
    };


    public Void setGenomicList(String sourceKey, List<GenomicOmicsMeta> omicsMetas){
        try {
            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

            for(GenomicOmicsMeta omicsmeta : omicsMetas) {
                String where = " WHERE omics_meta_id = " + omicsmeta.omicsMetaId;

                String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/copy_number_variation.sql");
                sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
                sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
                log.info("Query " + sql_statement);

                omicsmeta.copyNumberVariations = this.getSourceJdbcTemplate(source).query(sql_statement, cnvMapper);

                sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/structural_variation.sql");
                sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
                sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
                log.info("Query " + sql_statement);

                omicsmeta.structuralVariations = this.getSourceJdbcTemplate(source).query(sql_statement, svMapper);


                sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/mutation.sql");
                sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
                sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
                log.info("Query " + sql_statement);

                omicsmeta.mutations = this.getSourceJdbcTemplate(source).query(sql_statement, mutationMapper);

                sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/expression.sql");
                sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
                sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
                log.info("Query " + sql_statement);

                omicsmeta.expressions = this.getSourceJdbcTemplate(source).query(sql_statement, expressionMapper);

            }
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
        return null;
    }

    public List<GenomicOmicsMeta> getGenomicOmicsMeta(String sourceKey, Long id, OmicsMetaQueryType queryType, Boolean genomicdetail){

        String where = " WHERE omics_meta_id = " + id;
        if(queryType == OmicsMetaQueryType.SPECIMEN)
            where = " WHERE specimen_id = " + id;

        try {

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);


            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/omics_meta.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            List<GenomicOmicsMeta> omicsMetas = this.getSourceJdbcTemplate(source).query(sql_statement, omicsMetaRowMapper);

            if(genomicdetail)
                setGenomicList(sourceKey, omicsMetas);

            return omicsMetas;
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }


    private final RowMapper<Specimen> specimenRowMapper = new RowMapper<Specimen>() {
        @Override
        public Specimen mapRow(ResultSet rs, int rowNum) throws SQLException {
            Specimen specimen = new Specimen();
            specimen.personID = rs.getLong("person_id");
            specimen.specimenID = rs.getLong("specimen_id");
            specimen.specimenConceptID = rs.getInt("specimen_concept_id");
            specimen.specimenTypeConceptID = rs.getInt("specimen_type_concept_id");
            specimen.specimenDate = rs.getDate("specimen_date");
            // 5.2 버전
            //specimen.specimenDateTime = rs.getTimestamp("specimen_datetime");
            specimen.quantity = rs.getInt("quantity");
            specimen.unitConceptID = rs.getInt("unit_concept_id");
            specimen.anatomicSiteConceptID = rs.getInt("anatomic_site_concept_id");
            specimen.diseaseStatusConceptID = rs.getInt("disease_status_concept_id");
            specimen.specimenSourceID = rs.getString("specimen_source_id");
            specimen.specimenSourceValue = rs.getString("specimen_source_value");
            specimen.unitSourceValue = rs.getString("unit_source_value");
            specimen.anatomicSiteSourceValue = rs.getString("anatomic_site_source_value");
            specimen.diseaseStatusSourceValue = rs.getString("disease_status_source_value");
            return specimen;
        }
    };



    public List<Specimen> getSpecimen(String sourceKey, String id, Boolean is_omics_meta, SPECIMEN_QUERYTYPE querytype ){

        String query = "SELECT * FROM @CDM_schema.specimen WHERE specimen_id = " + id;

        if(querytype == SPECIMEN_QUERYTYPE.PERSON_ID){
            query = "SELECT * FROM @CDM_schema.specimen WHERE person_id = " + id;
        }else if(querytype == SPECIMEN_QUERYTYPE.OMICSMETA_ID){
            query = "SELECT * FROM @CDM_schema.specimen WHERE specimen_id in ( select specimen_id FROM @CDM_schema.genomic_omics_meta WHERE omics_meta_id = " + id +")";
        }

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String sql_statement = SqlRender.renderSql(query, new String[]{"CDM_schema"}, new String[]{tableQualifier});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
        log.info("Query " + sql_statement);

        List<Specimen> specimens = this.getSourceJdbcTemplate(source).query(sql_statement, specimenRowMapper);

        if(is_omics_meta) {
            for (Specimen specimen : specimens) {
                specimen.omicsMetas = getGenomicOmicsMeta(sourceKey, specimen.specimenID, OmicsMetaQueryType.SPECIMEN, Boolean.TRUE);
            }
        }

        return specimens;
    }


    /**
     * @param sourceKey
     * @param uriInfo
     *  Param List
     *      gene  :
     *
     * @return
     */

    @Path("/copynumbervariation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<CopyNumberVariation> getCopyNumberVariation(@PathParam("sourceKey") String sourceKey, @Context UriInfo uriInfo) {

        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String where = this.getWhereCopyNumberVariation(queryParams, tableQualifier);
        String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/copy_number_variation.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        return  this.getSourceJdbcTemplate(source).query(sql_statement, cnvMapper);
    }

    /**
     * @param sourceKey
     * @param uriInfo
     *  Param List
     *      gene  :
     *
     * @return
     */

    @Path("/structuralvariation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<StructuralVariation> getStructuralVariation(@PathParam("sourceKey") String sourceKey, @Context UriInfo uriInfo) {

        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String where = this.getWhereStructuralVariation(queryParams, tableQualifier);
        String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/structural_variation.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        return this.getSourceJdbcTemplate(source).query(sql_statement, svMapper);
    }

    /**
     * @param sourceKey
     * @param uriInfo
     *  Param List
     *      gene  :
     *
     * @return
     */

    @Path("/mutation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Mutation> getMutation(@PathParam("sourceKey") String sourceKey, @Context UriInfo uriInfo) {

        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String where = this.getWhereMutation(queryParams, tableQualifier);
        String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/mutation.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        return this.getSourceJdbcTemplate(source).query(sql_statement, mutationMapper);
    }

    /**
     * @param sourceKey
     * @param uriInfo
     *  Param List
     *      gene  :
     *
     * @return
     */

    @Path("/expression")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Expression> getExpression(@PathParam("sourceKey") String sourceKey, @Context UriInfo uriInfo) {

        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String where = this.getWhereMutation(queryParams, tableQualifier);
        String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/expression.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        return this.getSourceJdbcTemplate(source).query(sql_statement, expressionMapper);
    }

    /**
     * @param sourceKey
     * @param genomicType
     *   support in genomicType [ copynumbervariation, mutation, structuralvariation ]
     * @param uriInfo
     * @return
     */
    @Path("/{genomicType}/person")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Genomic> getCopyNumberVariationPerson(@PathParam("sourceKey") String sourceKey,@PathParam("genomicType") String genomicType, @Context UriInfo uriInfo) {

        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();


        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);


        String Query = "SELECT omics_meta_id FROM @CDM_schema.@Table @WHERE  GROUP BY omics_meta_id";

        if(genomicType.equals("copynumbervariation")){
            Query = SqlRender.renderSql(Query, new String[]{"CDM_schema","Table", "WHERE"}, new String[]{tableQualifier,"genomic_copy_number_variation as cnv ", this.getWhereCopyNumberVariation(queryParams, tableQualifier)});
        }
        else if(genomicType.equals("mutation")){
            Query = SqlRender.renderSql(Query, new String[]{"CDM_schema","Table", "WHERE"}, new String[]{tableQualifier,"genomic_mutation as mutation ", this.getWhereMutation(queryParams, tableQualifier)});

        }
        else if(genomicType.equals("structuralvariation")){
            Query = SqlRender.renderSql(Query, new String[]{"CDM_schema","Table", "WHERE"}, new String[]{tableQualifier,"genomic_structural_variation as sv ", this.getWhereStructuralVariation(queryParams, tableQualifier)});
        }

        return getSourceJdbcTemplate(source).query(Query, new RowMapper<Genomic>() {
            @Override
            public Genomic mapRow(final ResultSet rs, final int arg1) throws SQLException {
                Long specimenId = rs.getLong("specimen_id");

                final Genomic genomic = new Genomic();

                //genomic.person = getGenomicPerson(sourceKey, Long.toString(specimen.personID));
                //genomic.specimens = getSpecimen(sourceKey, Long.toString(specimenId), false);
                return genomic;
            }
        });
    }

    @Path("/person/{personId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Genomic getPersonGenomic(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId) {
        Genomic genomic = new Genomic();

        try {
            genomic.specimens = getSpecimen(sourceKey, personId, true, SPECIMEN_QUERYTYPE.PERSON_ID);
            genomic.person = getGenomicPerson(sourceKey, personId);
            return genomic;
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    @Path("/person/{personId}/omicsmeta")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<GenomicOmicsMeta> getPersonOmicsMeta(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId) {

        String where = "WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE person_id = " + personId +" )";

        try {

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/omics_meta.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            List<GenomicOmicsMeta> omicsMetas = this.getSourceJdbcTemplate(source).query(sql_statement, omicsMetaRowMapper);

            setGenomicList(sourceKey, omicsMetas);
            return omicsMetas;

        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    @Path("/person/{personId}/specimen")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Specimen> getPersonSpecimen(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId) {

        try {
            List<Specimen> specimens =  getSpecimen(sourceKey, personId, false, SPECIMEN_QUERYTYPE.PERSON_ID);
            return specimens;

        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }


    @Path("/person/{personId}/structuralvariation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<StructuralVariation> getPersonSV(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();
            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereStructuralVariation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  sv.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE persion_id = " + personId + "))";
            }else{
                where = "WHERE sv.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE persion_id = " + personId + "))";
            }
            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/structural_variation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, svMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }


    @Path("/person/{personId}/copynumbervariation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<CopyNumberVariation> getPersonCnv(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();

            Integer paramsize = queryParams.size();
            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereCopyNumberVariation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  cnv.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE person_id = " + personId + "))";
            }else{
                where = "WHERE cnv.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE person_id = " + personId + "))";
            }
            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/copy_number_variation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, cnvMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    @Path("/person/{personId}/mutation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Mutation> getPersonMutation(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();

            Integer paramsize = queryParams.size();
            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereMutation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  mutation.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE person_id = " + personId + "))";
            }else{
                where = "WHERE mutation.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE person_id = " + personId + "))";
            }


            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/mutation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, mutationMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }

    }



    @Path("/person/{personId}/expression")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Expression> getPersonExpression(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereExpression(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  expression.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE person_id = " + personId+ "))";
            }else{
                where = "WHERE expression.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id in (SELECT specimen_id FROM @CDM_schema.specimen WHERE person_id = " + personId + "))";
            }

            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/expression.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, expressionMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    @Path("/specimen/{specimenID}/structuralvariation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<StructuralVariation> getSpecimenSV(@PathParam("sourceKey") String sourceKey, @PathParam("specimenID") String specimenId, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereStructuralVariation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  sv.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id = " + specimenId+ ")";
            }else{
                where = "WHERE sv.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id = " + specimenId + ")";
            }

            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/structural_variation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, svMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }


    @Path("/specimen/{specimenID}/copynumbervariation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<CopyNumberVariation> getSpecimenCnv(@PathParam("sourceKey") String sourceKey, @PathParam("specimenID") String specimenID, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereCopyNumberVariation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  cnv.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id = " + specimenID+ ")";
            }else{
                where = "WHERE cnv.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id = " + specimenID + ")";
            }
            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/copy_number_variation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, cnvMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    @Path("/specimen/{specimenID}/mutation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Mutation> getSpecimenMutation(@PathParam("sourceKey") String sourceKey, @PathParam("specimenID") String specimenID, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereMutation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  mutation.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id = " + specimenID+ ")";
            }else{
                where = "WHERE mutation.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id = " + specimenID + ")";
            }
            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/mutation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, mutationMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }

    }



    @Path("/specimen/{specimenID}/expression")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Expression> getSpecimenExpression(@PathParam("sourceKey") String sourceKey, @PathParam("specimenID") String specimenID, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereExpression(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  expression.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id = " + specimenID+ ")";
            }else{
                where = "WHERE expression.omics_meta_id in ( SELECT omics_meta_id FROM @CDM_schema.genomic_omics_meta WHERE specimen_id = " + specimenID + ")";
            }

            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/expression.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, expressionMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }



    @Path("/specimen/{specimenID}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Genomic getSpecimenData(@PathParam("sourceKey") String sourceKey, @PathParam("specimenID") String specimenID) {
        Genomic genomic = new Genomic();
        try {
            List<Specimen> specimens = getSpecimen(sourceKey, specimenID, true, SPECIMEN_QUERYTYPE.SPECIMEN_ID);
            if(specimens.size() > 0) {
                genomic.specimens = specimens;
                genomic.person = getGenomicPerson(sourceKey, Long.toString(specimens.get(0).personID));
            }

            return genomic;
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }


    @Path("/omicsmeta/{omicsmetaID}/structuralvariation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<StructuralVariation> getOmicsmetaSV(@PathParam("sourceKey") String sourceKey, @PathParam("omicsmetaID") String omicsmetaID, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereStructuralVariation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  sv.omics_meta_id = " + omicsmetaID+ ")";
            }else{
                where = "WHERE sv.omics_meta_id = " + omicsmetaID + ")";
            }
            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/structural_variation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, svMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }


    @Path("/omicsmeta/{omicsmetaID}/copynumbervariation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<CopyNumberVariation> getOmicsmetaCnv(@PathParam("sourceKey") String sourceKey, @PathParam("omicsmetaID") String omicsmetaID, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereCopyNumberVariation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  cnv.omics_meta_id = " + omicsmetaID+ ")";
            }else{
                where = "WHERE cnv.omics_meta_id = " + omicsmetaID + ")";
            }
            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/copy_number_variation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, cnvMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    @Path("/omicsmeta/{omicsmetaID}/mutation")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Mutation> getOmicsmetaMutation(@PathParam("sourceKey") String sourceKey, @PathParam("omicsmetaID") String omicsmetaID, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereMutation(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  mutation.omics_meta_id = " + omicsmetaID+ ")";
            }else{
                where = "WHERE mutation.omics_meta_id = " + omicsmetaID + ")";
            }
            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/mutation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, mutationMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }

    }



    @Path("/omicsmeta/{omicsmetaID}/expression")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Expression> getOmicsmetaExpression(@PathParam("sourceKey") String sourceKey, @PathParam("omicsmetaID") String omicsmetaID, @Context UriInfo uriInfo) {

        try {
            MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
            Integer paramsize = queryParams.size();

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);
            String where = this.getWhereExpression(queryParams, tableQualifier);
            if(paramsize > 0){
                where = where + " and  expression.omics_meta_id = " + omicsmetaID+ ")";
            }else{
                where = "WHERE expression.omics_meta_id = " + omicsmetaID + ")";
            }
            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/expression.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);

            return this.getSourceJdbcTemplate(source).query(sql_statement, expressionMapper);


        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    @Path("/omicsmeta/{omicsmetaID}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public GenomicOmicsMeta getOmiceMetaData(@PathParam("sourceKey") String sourceKey, @PathParam("omicsmetaID") String omicsmetaID) {
        try {
            List<GenomicOmicsMeta> omicsMetas =  getGenomicOmicsMeta(sourceKey, Long.parseLong(omicsmetaID), OmicsMetaQueryType.OMICS, true);
            if(omicsMetas.size() > 0){
                return omicsMetas.get(0);
            }
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
        return null;
    }

    @Path("/omicsmeta/{omicsmetaID}/specimen")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Specimen getOmiceMetaSpecimen(@PathParam("sourceKey") String sourceKey, @PathParam("omicsmetaID") String omicsmetaID) {
        try {
            List<Specimen> specimens =  getSpecimen(sourceKey, omicsmetaID, true, SPECIMEN_QUERYTYPE.OMICSMETA_ID);
            if(specimens.size() > 0){
                return specimens.get(0);
            }
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
        return null;
    }


    private String getWhereParam(MultivaluedMap<String, String> queryParams, String tableQualifier, String column){
        List<String> params = queryParams.get(column);
        String ret = null;
        if(params != null && !params.isEmpty()){
            ret = tableQualifier+"."+ column +"  in  ( " +  this.JoinArray(listToArray(params)) + ")";
        }
        return ret;
    }

    public String getWhereExpression(MultivaluedMap<String, String> queryParams, String tableQualifier){
        List<String> gene = queryParams.get("gene");
        List<String> gene_concept_id = queryParams.get("gene_concept_id");
        List<String> expression_unit = queryParams.get("expression_unit");

        List<String> arrayWhere = new ArrayList<>();
        String where = "";
        String genein = "";

        if(gene != null && !gene.isEmpty()){
            genein = "expression.gene_concept_id in  ( SELECT concept_id FROM  @CDM_schema.concept WHERE concept_name in (" +  this.JoinArray(listToArray(gene)) + "))";
            arrayWhere.add(genein);
        } else if (gene_concept_id != null && !gene_concept_id.isEmpty()){
            genein = "expression.gene_concept_id in (" +  this.JoinArray(listToArray(gene_concept_id)) + ")";
            arrayWhere.add(genein);
        }

        if (gene_concept_id != null && !gene_concept_id.isEmpty()){
            genein = "expression.expression_unit in (" +  this.JoinArray(listToArray(expression_unit)) + ")";
            arrayWhere.add(genein);
        }

        if(!arrayWhere.isEmpty()) {
            where = " WHERE "+ ListAdapter.adapt(arrayWhere).makeString(" and ");
        }

        return SqlRender.renderSql(where, new String[]{"CDM_schema"}, new String[]{tableQualifier});
    }

    public String getWhereCopyNumberVariation(MultivaluedMap<String, String> queryParams, String tableQualifier){
        List<String> gene = queryParams.get("gene");
        List<String> gene_concept_id = queryParams.get("gene_concept_id");
        List<String> chromosome = queryParams.get("chromosome");

        List<String> arrayWhere = new ArrayList<>();
        String where = "";
        String genein = "";

        if(gene != null && !gene.isEmpty()){
            genein = "cnv.gene_concept_id in  ( SELECT concept_id FROM  @CDM_schema.concept WHERE concept_name in (" +  this.JoinArray(listToArray(gene)) + "))";
            arrayWhere.add(genein);
        } else if (gene_concept_id != null && !gene_concept_id.isEmpty()){
            genein = "cnv.gene_concept_id in (" +  this.JoinArray(listToArray(gene_concept_id)) + ")";
            arrayWhere.add(genein);
        }

        if (chromosome != null && !chromosome.isEmpty()){
            genein = "cnv.chromosome in (" +  this.JoinArray(listToArray(chromosome)) + ")";
            arrayWhere.add(genein);
        }

        if(!arrayWhere.isEmpty()) {
            where = " WHERE "+ ListAdapter.adapt(arrayWhere).makeString(" and ");
        }

        return SqlRender.renderSql(where, new String[]{"CDM_schema"}, new String[]{tableQualifier});
    }

    public String getWhereMutation(MultivaluedMap<String, String> queryParams, String tableQualifier){
        List<String> gene = queryParams.get("gene");
        List<String> gene_concept_id = queryParams.get("gene_concept_id");

        List<String> params = new ArrayList<>();
        params.add("chromosome");
        params.add("start_position");
        params.add("end_position");
        params.add("variant_classification");
        params.add("variant_type");
        params.add("reference_allele");
        params.add("mutation_status");
        params.add("hgvsp");
        params.add("allele_frequency");
        params.add("exon");

        List<String> arrayWhere = new ArrayList<>();
        String where = "";
        String genein = "";
        if(gene != null && !gene.isEmpty()){
            genein = "mutation.gene_concept_id in  ( SELECT concept_id FROM  @CDM_schema.concept WHERE concept_name in (" +  this.JoinArray(listToArray(gene)) + "))";
            arrayWhere.add(genein);
        } else if (gene_concept_id != null && !gene_concept_id.isEmpty()){
            genein = "mutation.gene_concept_id in (" +  this.JoinArray(listToArray(gene_concept_id)) + ")";
            arrayWhere.add(genein);
        }

        if(!arrayWhere.isEmpty()) {
            where = " WHERE "+ ListAdapter.adapt(arrayWhere).makeString(" and ");
        }

        for(String column : params) {
            String ret = this.getWhereParam(queryParams, "mutation", column);
            if(ret != null){
                where = where + " and " + ret;
            }
        }

        return SqlRender.renderSql(where, new String[]{"CDM_schema"}, new String[]{tableQualifier});
    }

    public String getWhereStructuralVariation(MultivaluedMap<String, String> queryParams, String tableQualifier){
        List<String> gene = queryParams.get("gene");
        List<String> gene_concept_id = queryParams.get("gene_concept_id");

        List<String> supporting_reads = queryParams.get("supporting_reads");

        List<String> arrayWhere = new ArrayList<>();
        String where = "";
        String genein = "";

        String concept = "";
        String supporting = "";

        if(gene != null && !gene.isEmpty()){
            genein = "sv.gene1_concept_id in ( SELECT concept_id FROM @CDM_schema.concept WHERE concept_name in (" +  this.JoinArray(listToArray(gene)) + "))";
            arrayWhere.add(genein);
            genein = "sv.gene2_concept_id in ( SELECT concept_id FROM @CDM_schema.concept WHERE concept_name in (" +  this.JoinArray(listToArray(gene)) + "))";
            arrayWhere.add(genein);
            genein = "sv.gene3_concept_id in ( SELECT concept_id FROM @CDM_schema.concept WHERE concept_name in (" +  this.JoinArray(listToArray(gene)) + "))";
            arrayWhere.add(genein);
            concept = ListAdapter.adapt(arrayWhere).makeString(" or ");
        } else if (gene_concept_id != null && !gene_concept_id.isEmpty()){
            genein = "sv.gene1_concept_id in (" +  this.JoinArray(listToArray(gene_concept_id)) + ")";
            arrayWhere.add(genein);
            genein = "sv.gene2_concept_id in (" +  this.JoinArray(listToArray(gene_concept_id)) + ")";
            arrayWhere.add(genein);
            genein = "sv.gene3_concept_id in (" +  this.JoinArray(listToArray(gene_concept_id)) + ")";
            arrayWhere.add(genein);
            concept = ListAdapter.adapt(arrayWhere).makeString(" or ");
        }

        if(supporting_reads != null && !supporting_reads.isEmpty()){
            arrayWhere.clear();

            genein = "sv.supporting_reads1 in ( " +  this.JoinArray(listToArray(gene)) + ")";
            arrayWhere.add(genein);
            genein = "sv.supporting_reads2 in ( " +  this.JoinArray(listToArray(gene)) + ")";
            arrayWhere.add(genein);
            genein = "sv.supporting_reads3 in ( " +  this.JoinArray(listToArray(gene)) + ")";
            arrayWhere.add(genein);
            supporting = ListAdapter.adapt(arrayWhere).makeString(" or ");
        }

        if(!arrayWhere.isEmpty()) {

            where = " WHERE "+ concept + supporting;
        }



        return SqlRender.renderSql(where, new String[]{"CDM_schema"}, new String[]{tableQualifier});
    }

    private static <T> String[] listToArray(List<T> list){
        String [] array = new String[list.size()];
        for (int i =0 ; i<array.length;i++)
            array[i] = list.get(i).toString();
        return array;
    }


    private static String JoinArray(final String[] array) {
        String result = "";

        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                result += ",";
            }

            result += "'" + array[i] + "'";
        }

        return result;
    }
}
