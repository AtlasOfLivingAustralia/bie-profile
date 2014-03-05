package org.ala.postprocess;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.List;

import javax.inject.Inject;

import org.ala.dao.TaxonConceptDao;
import org.ala.dto.ExtendedTaxonConceptDTO;
import org.ala.model.*;
import org.ala.util.SpringUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang.StringUtils;
import org.gbif.file.CSVReader;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.checklist.lucene.CBCreateLuceneIndex;
import au.org.ala.checklist.lucene.CBIndexSearch;
import au.org.ala.checklist.lucene.model.NameSearchResult;

/**
 * The great NSL name fixup - required for incorrectly marked "accepted" taxon concepts
 * 
 * 1) check to see if the superseded concept exists as accepted (and seee if it is one of the synonyms)
 * 
 * 2) If so copy across the cassandra non taxon data.  Delete the superseded value from the name index and cassandra
 * 
 * 3) Generate a list of lsids that need to be reprocessed on the biocache (after the revised name index has been copied across)
 * 
 * 4) The BIE search index will need to be regenerated too.  So that the deleted records are no longer in the index.
 * 
 * @author Natasha Quimby (natasha.quimby@csiro.au)
 *
 */
@Component("nslNameFix")
public class NSLNameFix {

    @Inject
    protected CBIndexSearch searcher;
    
    @Inject
    protected CBCreateLuceneIndex indexWriter;
    
    @Inject
    protected TaxonConceptDao taxonConceptDao;
    
    private static final String nslFile="/data/bie-staging/ala-names/nsl_superseded_accepted-utf8.csv";
    private static final String biocacheFile="/data/bie-staging/ala-names/biocache-superseded-values.csv";
    
    public static void main(String[] args) throws Exception{
        ApplicationContext context = SpringUtils.getContext();
        NSLNameFix l = context.getBean(NSLNameFix.class);
        boolean test = args.length>0 && args[0].equals("-test");
        boolean repair = args.length>0 && args[0].equals("-repair");
        if(repair){
            l.repairNameMatchingIndex();
        } else {
            l.processNames(test);
        }
        System.exit(0);
    }
    
    public void repairNameMatchingIndex() throws Exception{
        CSVReader reader = CSVReader.build(new File(biocacheFile), "UTF-8", ",", '"', 1);
        while(reader.hasNext()){
            String[] values = reader.next();
            String lsid = values[0];
            indexWriter.deleteName(lsid);
        }
        indexWriter.commit(true, true);
    }
    
    public void processNames(boolean test) throws Exception{
        if(test){
            System.out.println("Performing a test load");
        }
        CSVReader reader = CSVReader.build(new File(nslFile), "UTF-8", ",", '"', 1);
        CSVWriter writer = new CSVWriter(new FileWriterWithEncoding(biocacheFile, Charset.forName("UTF-8")));
        int count=0,delete=0,nosyn=0, noaccepted=0;
        while(reader.hasNext()){
            count++;
            String[] values = reader.next();
            String badLsid = values[1];
            String badNameLsid = values[2];
            String badSciName = values[3];
            String acceptedNameLsid = values[4];
            String acceptedLsid = values[5];
            String acceptedName = values[6];
            NameSearchResult nsrBad = searcher.searchForRecordByLsid(badLsid);
            NameSearchResult accepted = searcher.searchForRecordByLsid(searcher.getPrimaryLsid(acceptedLsid));
            //now get the synonyms for the accepted
            if(accepted != null){
                List<SynonymConcept> synonyms= taxonConceptDao.getSynonymsFor(accepted.getLsid());
                if(containsSynonym(badSciName, badNameLsid, synonyms)){
                    System.out.println("Concept " + badLsid +" is a synonym of " + acceptedLsid);
                    delete++;
                    if(!test){
                        //we need to actually perform the delete
                        ExtendedTaxonConceptDTO etc = taxonConceptDao.getExtendedTaxonConceptByGuid(badLsid, false);
                        if(etc != null){
                            //copy across the categories
                            if(etc.getCategories() != null){
                                for(Category c : etc.getCategories()){
                                    taxonConceptDao.addCategory(accepted.getLsid(), c);
                                }
                            }
                            //Copy across the Common names
                            if(etc.getCommonNames() != null){
                                for(CommonName cn:etc.getCommonNames()){
                                    taxonConceptDao.addCommonName(accepted.getLsid(), cn);
                                }
                            }
                            //copy across conservation statuses
                            if(etc.getConservationStatuses() != null){
                                for(ConservationStatus cs: etc.getConservationStatuses()){
                                    taxonConceptDao.addConservationStatus(accepted.getLsid(), cs);
                                }
                            }
                            //copy across the distribution images
                            if(etc.getDistributionImages() != null){
                                for(Image i : etc.getDistributionImages()){
                                    taxonConceptDao.addDistributionImage(accepted.getLsid(), i);
                                }
                            }
                            //copy acrosss the images
                            if(etc.getImages() != null){
                                for(Image i : etc.getImages()){
                                    taxonConceptDao.addImage(accepted.getLsid(), i);
                                }
                            }
                            //copy across the text properties
                            if(etc.getSimpleProperties() != null){
                                for(SimpleProperty sp: etc.getSimpleProperties()){
                                    taxonConceptDao.addTextProperty(accepted.getLsid(), sp);
                                }
                            }                           
                            //now delete the badlsid
                            taxonConceptDao.delete(badLsid);
                            indexWriter.deleteName(badLsid);
                            
                            //write the lsid and sci name to the biocache reprocess/index file
                            writer.writeNext(new String[]{badLsid, badSciName});
                                
                        } else{
                            System.out.println("Unable to locate the ETC for " + badLsid);
                        }
                    } else{
                        writer.writeNext(new String[]{badLsid, badSciName});
                    }
                } else{
                    System.out.println("NO MATCH: " + StringUtils.join(values,','));
                    nosyn++;
                }
            } else{
                System.out.println("Can't located accepted lsid in current names " + StringUtils.join(values,','));
                noaccepted++;
            }
            
        }
        System.out.println("Number of lines processed : " + count + " to be deleted: " + delete + " not a synonym of accepted: " + nosyn + " no accepted value: " + noaccepted);
        writer.flush();
        writer.close();
        indexWriter.commit(true,true);
    }
    
    private boolean containsSynonym(String name, String nameLsid, List<SynonymConcept> synonyms){
        for(SynonymConcept s : synonyms){
            if(nameLsid.equals(s.getNameGuid())){
                return true;
            }
            if(name.equals(s.getNameString())){
                return true;
            }
        }
        return false;
    }
    
}
