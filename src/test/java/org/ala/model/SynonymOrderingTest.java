package org.ala.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

public class SynonymOrderingTest extends TestCase{

    public void testIntYearCalculations(){
        SynonymConcept sc1 = new SynonymConcept();
        sc1.setAuthor("Schewiakoff, 1892");
        assertEquals(new Integer(1892), sc1.getIntYear());
        
        sc1.setAuthorYear("2000");
        assertEquals(new Integer(1892),sc1.getIntYear());///needs to return 1892 to prove that parsing is only done once.
        SynonymConcept sc2 = new SynonymConcept();
        sc2.setAuthor("Schewiakoff, 1892");
        sc2.setAuthorYear("2000");
        assertEquals(new Integer(2000),sc2.getIntYear());
    }
    
    public void testOrder(){
        SynonymConcept sc1 = createSynonym("Pachyseris gemmae", "Nemenzo, 1955", null);
        SynonymConcept sc2 = createSynonym("Pachyseris carinata", "BrŸggeman, 1879","1879");
        SynonymConcept sc3 = createSynonym("Agaricia rugosa", "Lamarck, 1816", "1816");
        SynonymConcept sc4 = createSynonym("Pachyseris torresiana","Vaughan, 1918",null);
        SynonymConcept sc5 = createSynonym("Pachyseris valenciennesi", "Milne-Edwards & Haime", "1918"); //this is the incorrect year for this name just using it to test alphabetical order after year
        SynonymConcept sc6 = createSynonym("Pachyseris torresiana","Vaughan",null);
        SynonymConcept sc7 = createSynonym("Pachyseris torresiana",null,null);
        TaxonConcept tc1 = new TaxonConcept();
        tc1.setNameString("Zus bus");
        
        assertTrue(sc1.compareTo(tc1)>0);
        assertTrue(tc1.compareTo(sc1)<0);
        
        
        List<SynonymConcept> list = new ArrayList<SynonymConcept>();
        list.add(sc1);
        list.add(sc2);
        list.add(sc3);
        list.add(sc4);        
        assertTrue(sc1.compareTo(sc2)>0);
        assertTrue(sc3.compareTo(sc2)<0);
        assertTrue(sc5.compareTo(sc4)>0);
        assertTrue(sc6.compareTo(sc4)>0);
        assertTrue(sc4.compareTo(sc6)<0);
        assertTrue(sc1.compareTo(sc1) == 0);
        assertTrue(sc7.compareTo(sc5)>0);
        System.out.println(list);
        Collections.sort(list);
        System.out.println(list);
    }
    
    private SynonymConcept createSynonym(String name, String author, String year){
        SynonymConcept sc = new SynonymConcept();
        sc.setNameString(name);
        sc.setAuthor(author);
        sc.setAuthorYear(year);
        return sc;
    }
  
}
