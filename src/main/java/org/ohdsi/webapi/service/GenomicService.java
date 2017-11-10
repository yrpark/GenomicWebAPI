package org.ohdsi.webapi.service;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

    public GenomicPerson getGenomicPerson(String sourceKey, String personID){
        GenomicPerson person = new GenomicPerson() ;

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/person.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "PERSON_ID"}, new String[]{tableQualifier, personID});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
            @Override
            public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                person.personId = rs.getInt("PERSON_ID");
                person.gender = rs.getString("GENDER_SOURCE_VALUE");
                person.yearOfBirth = rs.getInt("YEAR_OF_BIRTH");
                person.monthOfBirth = rs.getInt("MONTH_OF_BIRTH");
                person.dayOfBirth = rs.getInt("DAY_OF_BIRTH");
                person.deathDate = rs.getInt("DEATH_DATE");
                return null;
            }
        });


        sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/measurment.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "PERSON_ID"}, new String[]{tableQualifier,personID});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
            @Override
            public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                Measurement measurement = new Measurement();

                measurement.measurementConceptID = rs.getInt("MEASUREMENT_CONCEPT_ID");
                measurement.measurementDateTime = rs.getTimestamp("MEASUREMENT_DATETIME");
                measurement.valueAsNumber = rs.getInt("VALUE_AS_NUMBER");
                person.measurement.add(measurement);
                return null;
            }
        });


        sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/procedureoccurrence.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "PERSON_ID"}, new String[]{tableQualifier,personID});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
            @Override
            public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                ProcedureOccurrence occurrence = new ProcedureOccurrence();

                occurrence.procedureConceptId = rs.getInt("PROCEDURE_CONCEPT_ID");
                occurrence.procedureDateTime = rs.getTimestamp("PROCEDURE_DATETIME");
                person.procedureoccurrence.add(occurrence);
                return null;
            }
        });

        return person;
    }


    public Specimen getSpecimenGenomic(String sourceKey, String id){
        Specimen specimen = new Specimen();
        String where = " WHERE specimen_id = " + id;

        try {

            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

            String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/copy_number_variation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);
            getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
                @Override
                public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                    CopyNumberVariation report = new CopyNumberVariation();
                    report.cnvDataId = rs.getLong("CNV_DATA_ID");
                    report.updateDatetime = rs.getTimestamp("UPDATE_DATETIME");
                    report.panel = rs.getString("PANEL");
                    report.cliRrptId = rs.getString("CLI_RRPT_ID");
                    report.mutType = rs.getString("MUT_TYPE");
                    report.smpId = rs.getString("SMP_ID");
                    report.prjId = rs.getString("PRJ_ID");
                    report.secId = rs.getString("SEC_ID");
                    report.type = rs.getString("TYPE");
                    report.chromosome = rs.getString("CHROMOSOME");
                    report.start = rs.getLong("ST");
                    report.end = rs.getLong("EN");
                    report.geneConceptId = rs.getInt("GENE_CONCEPT_ID");
                    report.log2 = rs.getFloat("LOG2");
                    report.cn = rs.getInt("CN");
                    report.alteration = rs.getString("ALTERATION");
                    report.alterationDb = rs.getString("ALTERATION_DB");
                    report.sigflag = rs.getString("SIGFLAG");
                    report.hrd = rs.getString("HRD");
                    report.cnvType = rs.getString("CNVTYPE");
                    report.knownFlag = rs.getString("KNOWNFLAG");
                    report.specimenId = rs.getInt("SPECIMEN_ID");

                    specimen.cnv = report;
                    return null;
                }
            });


            sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/structural_variation.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

            getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
                @Override
                public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                    StructuralVariation report = new StructuralVariation();
                    report.svDataId = rs.getLong("SV_DATA_ID");
                    report.updateDatetime = rs.getTimestamp("UPDATE_DATETIME");
                    report.panel = rs.getString("PANEL");
                    report.cliRrptId = rs.getString("CLI_RRPT_ID");
                    report.mutType = rs.getString("MUT_TYPE");
                    report.smpId = rs.getString("SMP_ID");
                    report.prjId = rs.getString("PRJ_ID");
                    report.secId = rs.getString("SEC_ID");
                    report.type = rs.getString("TYPE");
                    report.geneConceptId = rs.getInt("GENE_CONCEPT_ID");
                    report.mismatches = rs.getString("MISMATCHES");
                    report.strands = rs.getString("STRANDS");
                    report.repOverlap = rs.getString("REP_OVERLAP");
                    report.svType = rs.getString("SV_TYPE");
                    report.readCount = rs.getString("READ_COUNT");
                    report.nkmers = rs.getLong("NKMERS");
                    report.discReadCount = rs.getLong("DISC_READ_COUNT");
                    report.breakpointCov = rs.getString("BREAKPOINT_COV");
                    report.contigId = rs.getString("CONTIG_ID");
                    report.contigSeq = rs.getString("CONTIG_SEQ");
                    report.alteration = rs.getString("ALTERATION");
                    report.alterationDb = rs.getString("ALTERATION_DB");
                    report.transType = rs.getString("TRANSTYPE");
                    report.rearrangementTarget = rs.getString("REARRANGEMENT_TARGET");
                    report.specimenId = rs.getInt("SPECIMEN_ID");

                    specimen.sv = report;
                    return null;
                }
            });


            sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/single_nucleotide_variants.sql");
            sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

            getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
                @Override
                public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                    SingleNucleotideVariants report = new SingleNucleotideVariants();
                    report.snvDataId = rs.getLong("SNV_DATA_ID");
                    report.updateDatetime = rs.getTimestamp("UPDATE_DATETIME");
                    report.panel = rs.getString("PANEL");
                    report.cliRrptId = rs.getString("CLI_RRPT_ID");
                    report.mutType = rs.getString("MUT_TYPE");
                    report.smpId = rs.getString("SMP_ID");
                    report.prjId = rs.getString("PRJ_ID");
                    report.secId = rs.getString("SEC_ID");
                    report.geneConceptId = rs.getInt("GENE_CONCEPT_ID");
                    report.ensId = rs.getString("ENS_ID");
                    report.mutationStatus = rs.getString("MUTATION_STATUS");
                    report.chromosome = rs.getString("CHROMOSOME");
                    report.ref = rs.getString("REF");
                    report.var = rs.getString("VAR");
                    report.variantClass = rs.getString("VARIANT_CLASS");
                    report.variantType = rs.getString("VARIANT_TYPE");
                    report.hgvsc = rs.getString("HGVSC");
                    report.hgvsp = rs.getString("HGVSP");
                    report.hgvspDb = rs.getString("HGVSP_DB");
                    report.start = rs.getLong("ST");
                    report.end = rs.getLong("EN");
                    report.dbsnp = rs.getString("DBSNP");
                    report.TTotalDepth = rs.getLong("T_TOTAL_DEPTH");
                    report.TRefDepth = rs.getLong("T_REF_DEPTH");
                    report.TVarDepth = rs.getLong("T_VAR_DEPTH");
                    report.NTotalDepth = rs.getLong("N_TOTAL_DEPTH");
                    report.NRefDepth = rs.getLong("N_REF_DEPTH");
                    report.NVarDepth = rs.getLong("N_VAR_DEPTH");
                    report.alleleFreq = rs.getFloat("ALLELE_FREQ");
                    report.strand = rs.getString("STRAND");
                    report.eoxn = rs.getString("EOXN");
                    report.intron = rs.getLong("INTRON");
                    report.sift = rs.getString("SIFT");
                    report.polyphen = rs.getString("POLYPHEN");
                    report.domain = rs.getString("DOMAIN");
                    report.hrd = rs.getString("HRD");
                    report.mmr = rs.getString("MMR");
                    report.zygosity = rs.getString("ZYGOSITY");
                    report.transcriptrank = rs.getInt("TRANSCRIPTRANK");
                    report.diagnosis = rs.getString("DIAGNOSIS");
                    report.drug = rs.getString("DRUG");
                    report.drugable = rs.getString("DRUGABLE");
                    report.whitelist = rs.getString("WHITELIST");
                    report.specimenId = rs.getInt("SPECIMEN_ID");

                    specimen.snv = report;
                    return null;
                }
            });

            return specimen;
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    public Specimen getSpecimen(String sourceKey, String specimenID, Boolean isgenomic){
        Specimen specimen ;
        if(isgenomic)
            specimen = getSpecimenGenomic(sourceKey, specimenID);
        else
            specimen = new Specimen();

        String query = "SELECT * FROM @CDM_schema.specimen WHERE specimen_id = " + specimenID;

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String sql_statement = SqlRender.renderSql(query, new String[]{"CDM_schema"}, new String[]{tableQualifier});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
        log.info("Query " + sql_statement);
        getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
            @Override
            public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                specimen.personID = rs.getInt("person_id");
                specimen.specimenConceptID = rs.getInt("specimen_concept_id");
                specimen.specimenTypeConceptID = rs.getInt("specimen_type_concept_id");
                specimen.specimenDate = rs.getDate("specimen_date");
                specimen.specimenDateTime = rs.getTimestamp("specimen_datetime");
                specimen.quantity = rs.getInt("quantity");
                specimen.unitConceptID = rs.getInt("unit_concept_id");
                specimen.anatomicSiteConceptID = rs.getInt("anatomic_site_concept_id");
                specimen.diseaseStatusConceptID = rs.getInt("disease_status_concept_id");
                specimen.specimenSourceID = rs.getString("specimen_source_id");
                specimen.specimenSourceValue = rs.getString("specimen_source_value");
                specimen.unitSourceValue = rs.getString("unit_source_value");
                specimen.anatomicSiteSourceValue = rs.getString("anatomic_site_source_value");
                specimen.diseaseStatusSourceValue = rs.getString("disease_status_source_value");

                return null;
            }
        });

        return specimen;
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

        return getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<CopyNumberVariation>() {
            @Override
            public CopyNumberVariation mapRow(final ResultSet rs, final int arg1) throws SQLException {
                final CopyNumberVariation report = new CopyNumberVariation();
                report.cnvDataId = rs.getLong("CNV_DATA_ID");
                report.updateDatetime = rs.getTimestamp("UPDATE_DATETIME");
                report.panel = rs.getString("PANEL");
                report.cliRrptId = rs.getString("CLI_RRPT_ID");
                report.mutType = rs.getString("MUT_TYPE");
                report.smpId = rs.getString("SMP_ID");
                report.prjId = rs.getString("PRJ_ID");
                report.secId = rs.getString("SEC_ID");
                report.type = rs.getString("TYPE");
                report.chromosome = rs.getString("CHROMOSOME");
                report.start = rs.getLong("ST");
                report.end = rs.getLong("EN");
                report.geneConceptId = rs.getInt("GENE_CONCEPT_ID");
                report.log2 = rs.getFloat("LOG2");
                report.cn = rs.getInt("CN");
                report.alteration = rs.getString("ALTERATION");
                report.alterationDb = rs.getString("ALTERATION_DB");
                report.sigflag = rs.getString("SIGFLAG");
                report.hrd = rs.getString("HRD");
                report.cnvType = rs.getString("CNVTYPE");
                report.knownFlag = rs.getString("KNOWNFLAG");
                report.specimenId = rs.getInt("SPECIMEN_ID");

                return report;
            }
        });
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

        return getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<StructuralVariation>() {
            @Override
            public StructuralVariation mapRow(final ResultSet rs, final int arg1) throws SQLException {
                final StructuralVariation report = new StructuralVariation();
                report.svDataId = rs.getLong("SV_DATA_ID");
                report.updateDatetime = rs.getTimestamp("UPDATE_DATETIME");
                report.panel = rs.getString("PANEL");
                report.cliRrptId = rs.getString("CLI_RRPT_ID");
                report.mutType = rs.getString("MUT_TYPE");
                report.smpId = rs.getString("SMP_ID");
                report.prjId = rs.getString("PRJ_ID");
                report.secId = rs.getString("SEC_ID");
                report.type = rs.getString("TYPE");
                report.geneConceptId = rs.getInt("GENE_CONCEPT_ID");
                report.mismatches = rs.getString("MISMATCHES");
                report.strands = rs.getString("STRANDS");
                report.repOverlap = rs.getString("REP_OVERLAP");
                report.svType = rs.getString("SV_TYPE");
                report.readCount = rs.getString("READ_COUNT");
                report.nkmers = rs.getLong("NKMERS");

                report.discReadCount = rs.getLong("DISC_READ_COUNT");
                report.breakpointCov = rs.getString("BREAKPOINT_COV");
                report.contigId = rs.getString("CONTIG_ID");
                report.contigSeq = rs.getString("CONTIG_SEQ");
                report.alteration = rs.getString("ALTERATION");
                report.alterationDb = rs.getString("ALTERATION_DB");
                report.transType = rs.getString("TRANSTYPE");
                report.rearrangementTarget = rs.getString("REARRANGEMENT_TARGET");
                report.specimenId = rs.getInt("SPECIMEN_ID");

                return report;
            }
        });
    }

    /**
     * @param sourceKey
     * @param uriInfo
     *  Param List
     *      gene  :
     *
     * @return
     */

    @Path("/singlenucleotidevariants")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<SingleNucleotideVariants> getSingleNucleotideVariants(@PathParam("sourceKey") String sourceKey, @Context UriInfo uriInfo) {

        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();

        Source source = getSourceRepository().findBySourceKey(sourceKey);
        String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        String where = this.getWhereSingleNucleotideVariants(queryParams, tableQualifier);
        String sql_statement = ResourceHelper.GetResourceAsString("/resources/genomic/sql/single_nucleotide_variants.sql");
        sql_statement = SqlRender.renderSql(sql_statement, new String[]{"CDM_schema", "WHERE_condition"}, new String[]{tableQualifier, where});
        sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());

        return getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<SingleNucleotideVariants>() {
            @Override
            public SingleNucleotideVariants mapRow(final ResultSet rs, final int arg1) throws SQLException {
                final SingleNucleotideVariants report = new SingleNucleotideVariants();
                report.snvDataId = rs.getLong("SNV_DATA_ID");
                report.updateDatetime = rs.getTimestamp("UPDATE_DATETIME");
                report.panel = rs.getString("PANEL");
                report.cliRrptId = rs.getString("CLI_RRPT_ID");
                report.mutType = rs.getString("MUT_TYPE");
                report.smpId = rs.getString("SMP_ID");
                report.prjId = rs.getString("PRJ_ID");
                report.secId = rs.getString("SEC_ID");
                report.geneConceptId = rs.getInt("GENE_CONCEPT_ID");
                report.ensId = rs.getString("ENS_ID");
                report.mutationStatus = rs.getString("MUTATION_STATUS");
                report.chromosome = rs.getString("CHROMOSOME");
                report.ref = rs.getString("REF");
                report.var = rs.getString("VAR");
                report.start = rs.getLong("ST");
                report.end = rs.getLong("EN");
                report.variantClass = rs.getString("VARIANT_CLASS");
                report.variantType = rs.getString("VARIANT_TYPE");
                report.hgvsc = rs.getString("HGVSC");
                report.hgvsp = rs.getString("HGVSP");
                report.hgvspDb = rs.getString("HGVSP_DB");
                report.dbsnp = rs.getString("DBSNP");
                report.TTotalDepth = rs.getLong("T_TOTAL_DEPTH");
                report.TRefDepth = rs.getLong("T_REF_DEPTH");
                report.TVarDepth = rs.getLong("T_VAR_DEPTH");
                report.NTotalDepth = rs.getLong("N_TOTAL_DEPTH");
                report.NRefDepth = rs.getLong("N_REF_DEPTH");
                report.NVarDepth = rs.getLong("N_VAR_DEPTH");
                report.alleleFreq = rs.getFloat("ALLELE_FREQ");
                report.strand = rs.getString("STRAND");
                report.eoxn = rs.getString("EOXN");
                report.intron = rs.getLong("INTRON");
                report.sift = rs.getString("SIFT");
                report.polyphen = rs.getString("POLYPHEN");
                report.domain = rs.getString("DOMAIN");
                report.hrd = rs.getString("HRD");
                report.mmr = rs.getString("MMR");
                report.zygosity = rs.getString("ZYGOSITY");
                report.transcriptrank = rs.getInt("TRANSCRIPTRANK");
                report.diagnosis = rs.getString("DIAGNOSIS");
                report.drug = rs.getString("DRUG");
                report.drugable = rs.getString("DRUGABLE");
                report.whitelist = rs.getString("WHITELIST");
                report.specimenId = rs.getInt("SPECIMEN_ID");
                return report;
            }
        });
    }

    /**
     * @param sourceKey
     * @param genomicType
     *   support in genomicType [ copynumbervariation, singlenucleotidevariants, structuralvariation ]
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


        String Query = "SELECT specimen_id FROM @CDM_schema.@Table @WHERE  GROUP BY specimen_id";

        if(genomicType.equals("copynumbervariation")){
            Query = SqlRender.renderSql(Query, new String[]{"CDM_schema","Table", "WHERE"}, new String[]{tableQualifier,"genomic_copy_number_variation as cnv ", this.getWhereCopyNumberVariation(queryParams, tableQualifier)});
        }
        else if(genomicType.equals("singlenucleotidevariants")){
            Query = SqlRender.renderSql(Query, new String[]{"CDM_schema","Table", "WHERE"}, new String[]{tableQualifier,"genomic_single_nucleotide_variants as snv ", this.getWhereSingleNucleotideVariants(queryParams, tableQualifier)});

        }
        else if(genomicType.equals("structuralvariation")){
            Query = SqlRender.renderSql(Query, new String[]{"CDM_schema","Table", "WHERE"}, new String[]{tableQualifier,"genomic_structural_variation as sv ", this.getWhereStructuralVariation(queryParams, tableQualifier)});
        }

        return getSourceJdbcTemplate(source).query(Query, new RowMapper<Genomic>() {
            @Override
            public Genomic mapRow(final ResultSet rs, final int arg1) throws SQLException {
                Long specimenId = rs.getLong("specimen_id");

                final Genomic genomic = new Genomic();
                Specimen specimen = getSpecimen(sourceKey, Long.toString(specimenId), false);
                genomic.person = getGenomicPerson(sourceKey, Integer.toString(specimen.personID));
                genomic.specimens.add(specimen);
                return genomic;
            }
        });
    }

    @Path("/person/{personId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Genomic getPersonGenomic(@PathParam("sourceKey") String sourceKey, @PathParam("personId") String personId) {
        Genomic genomic = new Genomic();
        String query = "SELECT specimen_id FROM @CDM_schema.specimen WHERE person_id = " + personId;

        try {
            Source source = getSourceRepository().findBySourceKey(sourceKey);
            String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

            String sql_statement = SqlRender.renderSql(query, new String[]{"CDM_schema"}, new String[]{tableQualifier});
            sql_statement = SqlTranslate.translateSql(sql_statement, "sql server", source.getSourceDialect());
            log.info("Query " + sql_statement);
            getSourceJdbcTemplate(source).query(sql_statement, new RowMapper<Void>() {
                @Override
                public Void mapRow(final ResultSet rs, final int arg1) throws SQLException {
                    Long specimenId = rs.getLong("specimen_id");
                    genomic.specimens.add(getSpecimen(sourceKey, Long.toString(specimenId), true));
                    return null;
                }
            });
            genomic.person = getGenomicPerson(sourceKey, personId);
            return genomic;
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }

    @Path("/specimen/{specimenID}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Genomic getSpecimen(@PathParam("sourceKey") String sourceKey, @PathParam("specimenID") String specimenID) {
        Genomic genomic = new Genomic();

        try {
            Specimen specimen = getSpecimen(sourceKey, specimenID, true);
            genomic.specimens.add(specimen);
            genomic.person = getGenomicPerson(sourceKey, Integer.toString(specimen.personID));
            return genomic;
        } catch (Exception exception) {
            throw new RuntimeException("Error getting therapy path reports" + exception.getMessage());
        }
    }



    public String getWhereCopyNumberVariation(MultivaluedMap<String, String> queryParams, String tableQualifier){
        List<String> gene = queryParams.get("gene");
        List<String> gene_concept_id = queryParams.get("gene_concept_id");

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

        if(!arrayWhere.isEmpty()) {
            where = " WHERE "+ ListAdapter.adapt(arrayWhere).makeString(" and ");
        }

        return SqlRender.renderSql(where, new String[]{"CDM_schema"}, new String[]{tableQualifier});
    }

    public String getWhereSingleNucleotideVariants(MultivaluedMap<String, String> queryParams, String tableQualifier){
        List<String> gene = queryParams.get("gene");
        List<String> gene_concept_id = queryParams.get("gene_concept_id");

        List<String> arrayWhere = new ArrayList<>();
        String where = "";
        String genein = "";
        if(gene != null && !gene.isEmpty()){
            genein = "snv.gene_concept_id in  ( SELECT concept_id FROM  @CDM_schema.concept WHERE concept_name in (" +  this.JoinArray(listToArray(gene)) + "))";
            arrayWhere.add(genein);
        } else if (gene_concept_id != null && !gene_concept_id.isEmpty()){
            genein = "snv.gene_concept_id in (" +  this.JoinArray(listToArray(gene_concept_id)) + ")";
            arrayWhere.add(genein);
        }

        if(!arrayWhere.isEmpty()) {
            where = " WHERE "+ ListAdapter.adapt(arrayWhere).makeString(" and ");
        }

        return SqlRender.renderSql(where, new String[]{"CDM_schema"}, new String[]{tableQualifier});
    }

    public String getWhereStructuralVariation(MultivaluedMap<String, String> queryParams, String tableQualifier){
        List<String> gene = queryParams.get("gene");
        List<String> gene_concept_id = queryParams.get("gene_concept_id");

        List<String> arrayWhere = new ArrayList<>();
        String where = "";
        String genein = "";

        if(gene != null && !gene.isEmpty()){
            genein = "sv.gene_concept_id in ( SELECT concept_id FROM @CDM_schema.concept WHERE concept_name in (" +  this.JoinArray(listToArray(gene)) + "))";
            arrayWhere.add(genein);
        } else if (gene_concept_id != null && !gene_concept_id.isEmpty()){
            genein = "sv.gene_concept_id in (" +  this.JoinArray(listToArray(gene_concept_id)) + ")";
            arrayWhere.add(genein);
        }

        if(!arrayWhere.isEmpty()) {
            where = " WHERE "+ ListAdapter.adapt(arrayWhere).makeString(" and ");
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
