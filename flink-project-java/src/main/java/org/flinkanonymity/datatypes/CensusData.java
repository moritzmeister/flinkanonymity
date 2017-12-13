package org.flinkanonymity.datatypes;

/**
 * CensusData
 * Datatype to contain 18 data parameters:
 * - YEAR - CENSUS YEAR
 * - DATANUM - Dataset number
 * - SERIAL - household serial number
 * - HHWT (household weight - indicates how many households in the US are represented by a given household)
 * - GQ - group quota status
 * - PERNUM - When combined with YEAR, DATANUM, and SERIAL, PERNUM uniquely identifies each person within the IPUMS.
 * - PERWT [NON IMPORTANT] (person weight - indicates how many persons in the US are represented by a given person)
 * - SEX
 * - AGE
 * - MARST - marital status
 * - RACE
 * - RACED - racedetails
 * - BPL - birthplace
 * - BPLD - birthplace details
 * - EDUC - EDUCATION
 * - EDUCD - EDUCATION DETAILS
 * - OCC - OCCUPATION
 * - INCWAGE - INCOME WAGE
 */

public class CensusData {
    public CensusData(int year, int datanum, int serial , int hhwt, int gq, int pernum, int perwt, int sex
            ,int age, int marst, int race, int raced, int bpl, int bpld, int educ, int educd, int occ, int incwage) {
        /* Constructs a CensusData Object from 18 integer inputs.*/

        this.year = year;
        this.serial = serial;
        this.datanum = datanum;
        this.hhwt = hhwt;
        this.gq = gq;
        this.pernum = pernum;
        this.perwt = perwt;
        this.sex = sex;
        this.age = age;
        this.marst = marst;
        this.race = race;
        this.raced = raced;
        this.bpl = bpl;
        this.bpld = bpld;
        this.educ = educ;
        this.educd = educd;
        this.occ = occ;
        this.incwage = incwage;

    }
    public CensusData(String line){
        /* Constructs a CensusData Object from a comma separated string input. */
        String[] args = line.split(",");

        if (args.length != 18) {
            throw new RuntimeException("Number of arguments does not equal 18: " + line);
        }

        this.year = Integer.parseInt(args[0]);
        this.serial = Integer.parseInt(args[1]);
        this.datanum = Integer.parseInt(args[2]);
        this.hhwt = Integer.parseInt(args[3]);
        this.gq = Integer.parseInt(args[4]);
        this.pernum = Integer.parseInt(args[5]);
        this.perwt = Integer.parseInt(args[6]);
        this.sex = Integer.parseInt(args[7]);
        this.age = Integer.parseInt(args[8]);
        this.marst = Integer.parseInt(args[9]);
        this.race = Integer.parseInt(args[10]);
        this.raced = Integer.parseInt(args[11]);
        this.bpl = Integer.parseInt(args[12]);
        this.bpld = Integer.parseInt(args[13]);
        this.educ = Integer.parseInt(args[14]);
        this.educd = Integer.parseInt(args[15]);
        this.occ = Integer.parseInt(args[16]);
        this.incwage = Integer.parseInt(args[17]);
    }

    public int year;
    public int datanum;
    public int serial;
    public int hhwt;
    public int gq;
    public int pernum;
    public int perwt;
    public int sex;
    public int age;
    public int marst;
    public int race;
    public int raced;
    public int bpl;
    public int bpld;
    public int educ;
    public int educd;
    public int occ;
    public int incwage;


    public String toString() {
        /* Returns the object attributes as a comma separated string */
        StringBuilder sb = new StringBuilder();
        sb.append("CensusData Object: ");
        sb.append(year).append(",");
        sb.append(datanum).append(",");
        sb.append(serial).append(",");
        sb.append(hhwt).append(",");
        sb.append(gq).append(",");
        sb.append(pernum).append(",");
        sb.append(perwt).append(",");
        sb.append(sex).append(",");
        sb.append(age).append(",");
        sb.append(marst).append(",");
        sb.append(race).append(",");
        sb.append(raced).append(",");
        sb.append(bpl).append(",");
        sb.append(bpld).append(",");
        sb.append(educ).append(",");
        sb.append(educd).append(",");
        sb.append(occ).append(",");
        sb.append(incwage);
        return sb.toString();
    }
}