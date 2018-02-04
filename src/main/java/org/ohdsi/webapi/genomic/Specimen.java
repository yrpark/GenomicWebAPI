package org.ohdsi.webapi.genomic;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by packyoungjin on 02/11/17.
 */
public class Specimen {
    public Long personID;
    public Long specimenID;
    public Integer specimenConceptID;
    public Integer specimenTypeConceptID;
    public Date specimenDate;
    public Timestamp specimenDateTime;
    public Integer quantity;
    public Integer unitConceptID;
    public Integer anatomicSiteConceptID;
    public Integer diseaseStatusConceptID;
    public String specimenSourceID;
    public String specimenSourceValue;
    public String unitSourceValue;
    public String anatomicSiteSourceValue;
    public String diseaseStatusSourceValue;

    public List<GenomicOmicsMeta> omicsMetas;


    public Specimen(){
        omicsMetas = new ArrayList<>();

    }
}
