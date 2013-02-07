package org.ala.hbase;

import java.io.FileReader;

import javax.inject.Inject;

import org.ala.dao.TaxonConceptDao;
import org.ala.util.SpringUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.checklist.lucene.model.NameSearchResult;
import au.org.ala.data.model.LinnaeanRankClassification;

/**
 * 
 * Loads the GUID's from the previous version of the BIE into the current BIE under the previousGuid column.
 * 
 * Relies on having a "Download" from the following query: http://bie.ala.org.au/search?q=*&fq=left:[*%20TO%20*]
 * 
 * 
 * @author Natasha
 *
 */
@Component("guidLoader")
public class PreviousGuidsLoader {
  @Inject
  protected TaxonConceptDao taxonConceptDao;
  public static void main(String[] args) throws Exception {

    ApplicationContext context = SpringUtils.getContext();
    PreviousGuidsLoader l = context.getBean(PreviousGuidsLoader.class);    
    long start = System.currentTimeMillis();
    l.load();
  }
  
  public void load() throws Exception{
      //go through the file and load it
      CSVReader tr = new CSVReader(new FileReader("/data/bie-staging/ala-names/sample-old-guids.csv"), ',', '"', '\\');
      String[] cols = tr.readNext(); //first line contains headers - ignore
      int lineNumber = 1;
      int identical=0,exception=0,notfound=0,newguid=0;
      while((cols=tr.readNext())!=null){
          //check to see if the guid exists 
          NameSearchResult nsr = taxonConceptDao.getNameResultByGuid(cols[0]);
          if(nsr == null){
              //System.out.println("Guid NOT found:" + cols[0]);
              LinnaeanRankClassification cl = new LinnaeanRankClassification();
              cl.setKingdom(StringUtils.trimToNull(cols[5]));
              cl.setPhylum(StringUtils.trimToNull(cols[6]));
              cl.setKlass(StringUtils.trimToNull(cols[7]));
              cl.setOrder(StringUtils.trimToNull(cols[8]));
              cl.setFamily(StringUtils.trimToNull(cols[9]));
              cl.setGenus(StringUtils.trimToNull(cols[10]));
              try{
                  nsr = taxonConceptDao.findCBDataByName(cols[1], cl, null);
                  if(nsr != null){
                      System.out.println("New name :" + nsr.getLsid());
                      newguid++;
                      String guid = nsr.getAcceptedLsid() != null ? nsr.getAcceptedLsid():nsr.getLsid();
                      taxonConceptDao.addIdentifier(guid, cols[0]);
                      taxonConceptDao.setPreviousVersionGuid(guid, cols[0]);
                  }
                  else
                      notfound++;
              }
              catch(Exception e){
                  exception++;
              }
          }
          else{
            //no need to set previousGuid because it is identical to the new guid
            identical++;
          }
          
      }
      System.out.println("FINISHED. Identical: " + identical + " Exception: " + exception + " Not Found: " + notfound + " NewGUID: " + newguid);
  }
}
