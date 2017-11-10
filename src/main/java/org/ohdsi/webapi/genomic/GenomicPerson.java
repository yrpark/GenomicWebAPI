package org.ohdsi.webapi.genomic;

import java.util.ArrayList;

/**
 * Created by packyoungjin on 07/10/17.
 */

public class GenomicPerson {
    public int  personId;
    public String gender;
    /**
     *   DD/MM/YYYY
     */
    public int yearOfBirth;
    public int monthOfBirth;
    public int dayOfBirth;
    public int deathDate;

    public ArrayList<Measurement> measurement;
    public ArrayList<ProcedureOccurrence> procedureoccurrence;


    public GenomicPerson(){
        measurement = new ArrayList<>();
        procedureoccurrence = new ArrayList<>();
    }
}
