package org.ohdsi.webapi.genomic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by packyoungjin on 07/10/17.
 */

public class Genomic {
    public GenomicPerson person;
    public List<Specimen> specimens;

    public Genomic(){
        person = new GenomicPerson();
        specimens = new ArrayList<>();
    }
}