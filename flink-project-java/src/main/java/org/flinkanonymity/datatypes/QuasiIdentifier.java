package org.flinkanonymity.datatypes;

import java.util.ArrayList;

public class QuasiIdentifier {

    public ArrayList<Generalization> qid = new ArrayList<Generalization>();

    public QuasiIdentifier(Generalization... args) {
        /* Constructs a Quasi Identifier from several Generalizations */
        for (Generalization attribute : args) {
            this.qid.add(attribute);
        }
    }

    public AdultData generalize(AdultData adult){
        /* Method to generalize an adult according to the previously specified QuasiIdendifier */
        for (Generalization gen : this.qid) {
            adult.setAttribute(gen.attr, gen.getHierarchy(adult.getAttribute(gen.attr)));
        }
        return adult;
    }
}


