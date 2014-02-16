package org.ala.report;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.ala.dao.StoreHelper;
import org.ala.dao.TaxonConceptDao;
import org.ala.dto.SpeciesProfileDTO;
import org.ala.model.Classification;
import org.ala.model.Habitat;
import org.ala.util.ColumnType;
import org.ala.util.SpringUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import au.com.bytecode.opencsv.CSVWriter;
/**
 * Generates the CSV dump to use for the taxon profile information.
 * 
 * @author Natasha Carter (natasha.carter@csiro.au)
 *
 */
@Component
public class BIEProfileDump {
    
    @Inject
    protected TaxonConceptDao taxonConceptDao;
    
    @Inject
    protected StoreHelper storeHelper;

    public static void main(String[] args) throws Exception{
        ApplicationContext context = SpringUtils.getContext();
        BIEProfileDump reportGen = ( BIEProfileDump) context.getBean( BIEProfileDump.class);
        reportGen.download();
        //reportGen.reloadHabitats();
        System.exit(0);
    }
    
    private void reloadHabitats(){
      ColumnType[] subColumns = new ColumnType[]{ColumnType.TAXONCONCEPT_COL, ColumnType.TAXONNAME_COL, ColumnType.IS_AUSTRALIAN, ColumnType.HABITAT_COL};
      Map<String, Map<String,Object>> rowMaps = storeHelper.getPageOfSubColumns("tc", subColumns, "urn:lsid:biodiversity.org.au:afd.taxon", 1000);
      
      String startKey="urn:lsid:biodiversity.org.au:afd.taxon";
      while(rowMaps.size() > 0){
          for(String rowKey : rowMaps.keySet()){
              Map<String,Object> map = rowMaps.get(rowKey);
              startKey = rowKey;
              if(map.containsKey(ColumnType.HABITAT_COL.getColumnName())){
                  List<Habitat>habitats = (List<Habitat>)map.get(ColumnType.HABITAT_COL.getColumnName());
                  java.util.TreeSet<Habitat> set = new java.util.TreeSet<Habitat>();
                  set.addAll(habitats);
                  if(habitats.size() != set.size() && set.size()>0){
                      //we need to reload the habitats for this one
                      System.out.println("Reloading the habitats for : " + rowKey + " " + habitats.size() + " ; " + set.size());
                      habitats = new java.util.ArrayList<Habitat>();
                      habitats.addAll(set);
                      try{
                        //System.out.println("new size: " + habitats.size());
                       taxonConceptDao.addHabitat(rowKey, habitats);
                      }
                      catch(Exception e){
                        System.out.println("Issue updating " + rowKey + ". " + e.getMessage());
                        e.printStackTrace();
                      }
                  }
              }
          }
          rowMaps =  storeHelper.getPageOfSubColumns("tc", subColumns, startKey, 1000);
      }
      System.out.println("Finished reloading the habitats");
    }

    private void download()throws Exception{
        CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream("/data/biocache/ALAtaxon_profile.csv")), '\t', '"');
        try{
            csvWriter.writeNext(new String[]{
                    "GUID",
                    "Scientific Name",
                    "Left",
                    "Right",
                    "Rank",
                    "Common Name",
                    "Conservation Status",
                    "Sensitive Status"
            });
            String startKey = "";//"urn:lsid:biodiversity.org.au:afd.taxon";
            int pageSize = 100;
            
            List<SpeciesProfileDTO> profiles = taxonConceptDao.getProfilePage(startKey, pageSize);
            while(profiles != null && profiles.size()>0){
                for(SpeciesProfileDTO profile : profiles){
                    System.out.println(profile.getGuid());
                    String name = profile.getScientificName();
                    if (name.startsWith("\"") && name.endsWith("\"")){
                        name = name.substring(1,  name.length()-1);
                    }
                    String[] values = new String[]{
                      profile.getGuid(),      
                      name,
                      profile.getLeft(),
                      profile.getRight(),
                      profile.getRank(),
                      profile.getCommonName(),
                      StringUtils.join(profile.getHabitats(),(",")),
                      StringUtils.join(profile.getConservationStatus(),(",")),
                      StringUtils.join(profile.getSensitiveStatus(),(","))
                      
                    };
                    csvWriter.writeNext(values);
                    startKey = profile.getGuid();
                }
                profiles = taxonConceptDao.getProfilePage(startKey, pageSize);
            }
            System.out.println("Last value " + startKey );
        }
        catch (Exception e) {
           e.printStackTrace();
        }
        finally{
            if(csvWriter != null){
                csvWriter.flush();
                csvWriter.close();
            }
        }
        
    }
    
    
}
