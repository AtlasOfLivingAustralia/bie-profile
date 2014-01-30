package org.ala.hbase;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.ala.dao.InfoSourceDAO;
import org.ala.dao.TaxonConceptDao;
import org.ala.model.Publication;
import org.ala.model.SynonymConcept;
import org.ala.model.TaxonConcept;
import org.ala.model.TaxonName;
import org.ala.util.LoadUtils;
import org.ala.util.SpringUtils;
import org.apache.log4j.Logger;
import org.gbif.dwc.record.DarwinCoreRecord;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * This class will load the details that are contained within a DWCA
 * 
 * @author Natasha Quimby (natasha.quimby@csiro.au)
 *
 */
@Component("dwcaLoader")
public class DwCALoader {
    protected static Logger logger  = Logger.getLogger(ANBGDataLoader.class);
    @Inject
    protected InfoSourceDAO infoSourceDAO;
    @Inject
    protected TaxonConceptDao taxonConceptDao;
    
    public static void main(String[] args) throws Exception {
        logger.info("Starting DWCA load....");
        
        ApplicationContext context = SpringUtils.getContext();
        DwCALoader loader = (DwCALoader) context.getBean(DwCALoader.class);
        for(String archive:args){
            logger.info("Starting to load " + archive);
            long start = System.currentTimeMillis();
            loader.load(archive);
            long finish = System.currentTimeMillis();
            logger.info("Data loaded in: "+((finish-start)/60000)+" minutes.");
        }
        System.exit(1);
    }
    /**
     * Loads the supplied archive
     * @param file The absolute path to the DWCA directory to load
     */
    public void load(String file) throws Exception{
        //open the archive
        Archive archive = ArchiveFactory.openArchive(new File(file));
        LoadUtils loadUtils = new LoadUtils();
        Iterator<DarwinCoreRecord> it =archive.iteratorDwc();
        //String[] status = new String[]{"[Not in Australia]"}
        while(it.hasNext()){
            DarwinCoreRecord record = it.next();
            //check to see if the record 
            String id = record.getId();
            String taxonId = record.getTaxonConceptID();
            String acceptedTaxonId = record.getAcceptedNameUsageID();
            String recordId = taxonId != null ?taxonId:id;
            //now get the accepted guid to add the values to
            String guid = acceptedTaxonId != null ?acceptedTaxonId:taxonId != null ?taxonId:id;
            //check to see if it is a guid in our system
            boolean lookedup=false;
            if(loadUtils.getAlaAcceptedSource(guid) == null){
                lookedup=true;
                guid = getLsid(record);
                if(guid != null){
                    //load the details
                }
            } else{
                //we know that we are loading extra information about a list that was merged into NSL
                if(!recordId.equals(acceptedTaxonId)){
                    //get the synonyms
                    List<SynonymConcept> synonyms =taxonConceptDao.getSynonymsFor(guid);
                    
                    String synId =taxonId != null? taxonId :id;
                    
                    SynonymConcept sc = getMatchingConcept(synonyms, synId);
                    if(sc != null){
                        sc.setAcceptedConceptGuid(acceptedTaxonId);                    
                        sc.setNameString(record.getScientificName());
                        sc.setAuthor(record.getScientificNameAuthorship());
                        //String occurrenceStatus = record.getOccurrenceStatus();
                    //boolean excluded = occurrenceStatus != null && !"Endemic".equalsIgnoreCase(occurrenceStatus);
                    //sc.setIsExcluded(excluded);
                        sc.setNameGuid(record.getScientificNameID());
                        sc.setPublishedIn(record.getNamePublishedIn());
                        sc.setNomenclaturalStatus(record.getNomenclaturalStatus());
                        //now reload all the synonyms
                        taxonConceptDao.setSynonymsFor(guid, synonyms);
                    }                                       
                } else{
                    // additional taxon concept, taxon name and publication details
                    TaxonConcept tc = taxonConceptDao.getByGuid(guid);
                    if(tc != null){
                        tc.setNameString(record.getScientificName());
                        tc.setAuthor(record.getScientificNameAuthorship());
                        tc.setNameGuid(record.getScientificNameID());
                        tc.setPublishedIn(record.getNamePublishedIn());
                        tc.setNomenclaturalStatus(record.getNomenclaturalStatus());
                        taxonConceptDao.create(tc);
                        //now add the extra taxon name details
                        TaxonName tn = new TaxonName();
                        tn.setAuthorship(record.getScientificNameAuthorship());
                        tn.setNomenclaturalStatus(record.getNomenclaturalStatus());
                        tn.setNomenclaturalCode(record.getNomenclaturalCode());
                        tn.setPublishedIn(record.getNamePublishedIn());
                        tn.setRankString(record.getTaxonRank());
                        taxonConceptDao.addTaxonName(guid, tn);
                        //now add the publication details
                        Publication p = new Publication();
                        p.setGuid(record.getNamePublishedInID());
                        p.setTitle(record.getNamePublishedIn());
                        p.setYear(record.getNamePublishedInYear());
                        taxonConceptDao.addPublication(guid, p);
                        
                    } else{
                        System.out.println("Can't locate " + guid);
                    }
                }
            }
            
            
        }
    }
    private void loadTaxonConceptDetails(DarwinCoreRecord record, String guid, String acceptedLsid){
        
    }
    private void loadTaxonNameDetails(DarwinCoreRecord record, String guid, String acceptedLsid){
        
    }
    private void loadPublicationDetails(DarwinCoreRecord record, String guid, String acceptedLsid){
        
    }
    private String getLsid(DarwinCoreRecord record){
        //TODO look up scientific name and return the corresponding LSID 
        return null;
    }
    private SynonymConcept getMatchingConcept(List<SynonymConcept> synonyms, String synId){
        for(SynonymConcept sc : synonyms){
            if(synId.equals(sc.getGuid())){
                return sc;
            }
        }
        return null;
    }
    private class GuidComparator implements Comparator<TaxonConcept>{

        @Override
        public int compare(TaxonConcept tc1, TaxonConcept tc2) {
            // TODO Auto-generated method stub
            return tc1.getGuid().compareTo(tc2.getGuid());
        }
        
    }

}
