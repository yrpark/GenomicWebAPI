package org.ohdsi.webapi.genomic;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by packyoungjin on 07/10/17.
 */

public class Genomic {
    public GenomicPerson person;
    public ArrayList<Specimen> specimens;

    public Genomic(){
        person = new GenomicPerson();
        specimens = new ArrayList<>();
    }
}