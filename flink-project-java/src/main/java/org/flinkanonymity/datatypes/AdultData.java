package org.flinkanonymity.datatypes;

public class AdultData {
/**
 * AdultData
 * Datatype to contain 9 data parameters:
 * - sex
 * - age
 * - race
 * - marital-status
 * - education
 * - native-country
 * - workclass
 * - occupation
 * - salary-class
 */

    public String sex;
    public String age;
    public String race;
    public String marst;
    public String educ;
    public String country;
    public String workclass;
    public String occ;
    public String salary;


    public AdultData(){
        this.sex = "";
        this.age = "";
        this.race = "";
        this.marst = "";
        this.educ = "";
        this.country = "";
        this.workclass = "";
        this.occ = "";
        this.salary = "";
    }

    public AdultData(String sex, String age, String race, String marst, String educ, String country, String workclass,
                      String occ, String salary) {
        /* Constructs a AdultData Object from 9 String inputs.*/
        this.sex = sex;
        this.age = age;
        this.race = race;
        this.marst = marst;
        this.educ = educ;
        this.country = country;
        this.workclass = workclass;
        this.occ = occ;
        this.salary = salary;

    }

    public AdultData(String line){
        /* Constructs a CensusData Object from a comma separated string input. */
        String[] args = line.split(",");

        if (args.length != 9) {
            throw new RuntimeException("Number of arguments does not equal 9: " + line);
        }

        this.sex = args[0];
        this.age = args[1];
        this.race = args[2];
        this.marst = args[3];
        this.educ = args[4];
        this.country = args[5];
        this.workclass = args[6];
        this.occ = args[7];
        this.salary = args[8];
    }

    public void setAttribute(String attribute, String value) {
        /* Sets the attribute of an adult to a specified value */
        switch (attribute){
            case "sex":
                this.sex = value;
                break;
            case "age":
                this.age = value;
                break;
            case "race":
                this.race = value;
                break;
            case "marst":
                this.marst = value;
                break;
            case "educ":
                this.educ = value;
                break;
            case "country":
                this.country = value;
                break;
            case "workclass":
                this.workclass = value;
                break;
            case "occ":
                this.occ = value;
                break;
            case "salary":
                this.salary = value;
                break;
            default:
                throw new IllegalArgumentException("Invalid attribute: " + attribute);
        }
    }

    public String getAttribute(String attribute) {
        /* Returns the value corresponding to an attribute of the adult */
        String temp;

        switch (attribute){
            case "sex":
                temp = this.sex;
                break;
            case "age":
                temp =  this.age;
                break;
            case "race":
                temp =  this.race;
                break;
            case "marst":
                temp =  this.marst;
                break;
            case "educ":
                temp =  this.educ;
                break;
            case "country":
                temp =  this.country;
                break;
            case "workclass":
                temp =  this.workclass;
                break;
            case "occ":
                temp =  this.occ;
                break;
            case "salary":
                temp =  this.salary;
                break;
            default:
                throw new IllegalArgumentException("Invalid attribute: " + attribute);
        }

        return temp;
    }

    public String toString() {
        /* Returns the object attributes as a comma separated string */
        StringBuilder sb = new StringBuilder();
        sb.append("CensusData Object: ");
        sb.append(sex).append(",");
        sb.append(age).append(",");
        sb.append(race).append(",");
        sb.append(marst).append(",");
        sb.append(educ).append(",");
        sb.append(country).append(",");
        sb.append(workclass).append(",");
        sb.append(occ).append(",");
        sb.append(salary);
        return sb.toString();
    }

}