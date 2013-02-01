
package org.ala.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

/**
 *
 * A POJO for a synonym - an extension of a Taxon Concept to include synonym type information
 *
 * @author Natasha Carter
 */
public class SynonymConcept extends TaxonConcept{
    protected Integer type;
    protected String relationship;
    protected String description;
    private Integer year=null; //used to store the numeric representation of the year so that it doesn't need to be recalculated every comparison
    Pattern yearPattern = Pattern.compile("([\\x00-\\x7F\\s]*)([12][0-9][0-9][0-9])([\\x00-\\x7F\\s]*)");
    
    
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }
    @Override
    public String toString() {
      return "SynonymConcept [type=" + type + ", relationship=" + relationship
          + ", description=" + description + ", year=" + year
          + ", guid=" + guid
          + ", acceptedConceptGuid=" + acceptedConceptGuid + ", nameString="
          + nameString + ", author=" + author + ", authorYear=" + authorYear
          + "]";
    }
//    public String toString(){
//        StringBuilder builder = new StringBuilder();
//        builder.append("SynonymConcept[type ").append(type.toString());
//        builder.append(", relationship ").append(relationship);
//        builder.append(", description").append(description);
//        builder.append(super.toString());
//        builder.append("]");
//        return builder.toString();
//    }

    @JsonIgnore
    public Integer getIntYear(){
        if(year == null){
            //initialise it based on first the year and then the authorship string
            if(StringUtils.isNotBlank(this.authorYear)){
                try{
                    year = Integer.parseInt(this.authorYear.trim());
                }
                catch(NumberFormatException e){
                    year =-1;
                }
            }
            else if(this.author != null){
                //search for a year in the authorship string
                Matcher matcher = yearPattern.matcher(this.author);
                if(matcher.matches()){                    
                    try{
                        year = Integer.parseInt(matcher.group(2));
                    }
                    catch(NumberFormatException e){
                        year = -1;
                    }
                }
                else
                    year = -1;
            }
            else
                year = -1;
        }
        return year;
    }   
    
    /**
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     * 
     * Want synonyms order by name but having the preferred concept first when multiple names.
     * Synonyms should be ordered by year of publication...
     * 
     */
    @Override
    public int compareTo(TaxonConcept o) {
        //if o is not a Synonym the TaxonConcept always takes precedence
        if(!(o  instanceof SynonymConcept))
          return 1;
      
        //first check the years
        if(!(o  instanceof SynonymConcept) ||this.getIntYear().equals(((SynonymConcept)o).getIntYear())){
            //check the names
            if(o.getNameString()!=null && nameString!=null){
                if(o.getNameString().equals(nameString)){
                    if(o.isPreferred != isPreferred){
                        if(isPreferred)
                            return -1;
                        else return 1;
                    }                
                }
                return nameString.compareTo(o.getNameString());
            }
            return -1;
        }
        else{
            if(getIntYear().intValue() == -1)
                return 1;
            if(((SynonymConcept)o).getIntYear().intValue() == -1)
              return -1;
            return this.getIntYear().compareTo(((SynonymConcept)o).getIntYear());
        }
    }   


}
