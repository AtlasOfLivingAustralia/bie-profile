package org.ala.hbase;

import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.inject.Inject;

import org.ala.dao.SolrUtils;
import org.ala.dao.TaxonConceptDao;
import org.ala.util.PartialIndex;
import org.ala.util.SpringUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import au.com.bytecode.opencsv.CSVReader;

/**
 * This class loads data from the new biocache dynamically through WS calls to the biocache
 * 
 * 
 * 
 * @author Natasha Carter (Natasha.Carter@csiro.au)
 */
@Component("biocacheDynamicLoader")
public class BiocacheDynamicLoader {
    @Inject
        protected TaxonConceptDao taxonConceptDao;
    @Inject
    protected SolrUtils solrUtils;
    private long start = System.currentTimeMillis();
    protected static Logger logger  = Logger.getLogger(BiocacheDynamicLoader.class);
    //http://biocache.ala.org.au/occurrences/search?q=!rank:species%20!rank:genus%20!rank:family%20!rank:order%20!rank:class%20!rank:phylum%20!rank:kingdom&facets=rank
    private String suffix = "http://biocache.ala.org.au/ws/occurrences/facets/download?q={0}&fq={1}&count=true&facets={2}";     
    private String[] geoLoads = new String[]{"taxon_concept_lsid","species_guid", "genus_guid","family","order","class","phylum","kingdom"};
    private String minorTaxonRanksUrlSuffix = "http://biocache.ala.org.au/ws/occurrences/facets/download?q={0}%20!rank:species%20!rank:genus%20!rank:family%20!rank:order%20!rank:class%20!rank:phylum%20!rank:kingdom&fq={1}&count=true&facets={2}";
    private String geoSpatial ="lat_long:%5B*+TO+*%5D";
    private String all = "*:*";
    //private static String minorTaxonRanksUrlSuffix2 = "http://biocache.ala.org.au/ws/occurrences/facets/download?q={0}%20!rank:species%20!rank:genus%20!rank:family%20!rank:order%20!rank:class%20!rank:phylum%20!rank:kingdom&count=true&facets=";
    public static void main(String[] args) throws Exception {
        ApplicationContext context = SpringUtils.getContext();
        BiocacheDynamicLoader l = context.getBean(BiocacheDynamicLoader.class);
        if(args.length >=1){
            String file = args[0];
            int threads = (args.length >1)? Integer.parseInt(args[1]):5;
            String fq="";
            if(args.length>2){
                SimpleDateFormat sfd = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                StringBuffer sb = new StringBuffer("last_processed_date:%5B");
                Date currentDate = new Date();
                Date revisedDate = currentDate;
                if(args[2].equals("lastDay"))
                    revisedDate = DateUtils.addDays(currentDate, -1);
                else if(args[2].equals("lastWeek"))
                    revisedDate = DateUtils.addDays(currentDate, -7);
                else if(args[2].equals("lastMonth"))
                  revisedDate = DateUtils.addMonths(currentDate, -1);
                sb.append(sfd.format(revisedDate));
                sb.append("%20TO%20*");
                sb.append("%5D");
                fq = sb.toString();
                //String fq =args.length>2 && args[2].equals("lastDay")?"last_processed_date[ TO*]":"";
            }
            l.load(file, fq, threads);
            //now reindex
            PartialIndex p = context.getBean(PartialIndex.class);
            p.process(file);
        }
        //l.load(5);
        //System.out.println(MessageFormat.format(minorTaxonRanksUrlSuffix2, "*:*"));
        System.exit(0);
    }
    public void load(String reindexFile, String fq,int workers) throws Exception{
        logger.info("Starting to reload the biocache counts with " + reindexFile + " and " + workers + "threads" );
        //queue up all the values
        ArrayBlockingQueue<String[]> valuesQueue = new ArrayBlockingQueue<String[]>(workers +1);
        ProcessingThread[] threads = new ProcessingThread[workers];
        FileWriter writer = new FileWriter(reindexFile);
        int i =0;
        while(i < workers){
            threads[i] = new ProcessingThread(valuesQueue, writer,i);
            threads[i].start();
            i++;
        }
        
        //construct the work threads that will be responsible for loading the counts into cassandra
        //The "all" queried version of the URL will write the lsids to file to allow reindexing. This shoudl allow both to be reindexed as all geospatial records will have a normal count too
        for(String rank : geoLoads){
            try{
                valuesQueue.put(new String[]{all, fq, rank});
                valuesQueue.put(new String[]{geoSpatial, fq, rank});
            }
            catch(Exception e){
                
            }
        }
        for(ProcessingThread t : threads)
            t.running=false;
        for(Thread t : threads)
            t.join();
        
        writer.flush();
        writer.close();
        logger.info("Finished loading the counts.");
       
      
        //so we need to load all the information from WS's
//        HttpClient httpClient = new HttpClient();
//        ArrayBlockingQueue<String[]> lsidQueue = new ArrayBlockingQueue<String[]>(50);
//        List<SolrInputDocument>docs = Collections.synchronizedList(new ArrayList<SolrInputDocument>(100));
//        IndexingThread primaryThread = new IndexingThread(lsidQueue, docs, 1);
//        new Thread(primaryThread).start();
//        IndexingThread[] otherThreads = new IndexingThread[workers-1];
//        int i =0;
//        while(workers >1){
//            IndexingThread it = new IndexingThread(lsidQueue, docs, workers--);
//            otherThreads[i++] =it;
//            new Thread(it).start();
//           
//        }
//        
//        for(String load : geoLoads){
//            String loadUrl = load.equals("taxon_concept_lsid")?minorTaxonRanksUrlSuffix + load:suffix+load ;
//            logger.info("Starting to reload " + loadUrl);
//                   
//            try{                
//                GetMethod gm = new GetMethod(loadUrl); 
//                logger.info("Response code for get method: " +httpClient.executeMethod(gm));
//                
//                CSVReader reader = new CSVReader(new InputStreamReader(gm.getResponseBodyAsStream(), gm.getResponseCharSet()));
//                String[] values = reader.readNext();
//                logger.info("values: " + values.length);
//                boolean lookup = !(values[0].contains("lsid") || values[0].contains("guid"));
//                primaryThread.setLookup(lookup);
//                for(IndexingThread it : otherThreads)
//                    it.setLookup(lookup);
//                values =reader.readNext();
//                while(values!= null){
//                    if(values.length == 2){
//                        String lsid = lookup?taxonConceptDao.findLsidByName(values[0]):values[0];
//                        if(lsid != null && lsid.length()>0){                            
//                            lsidQueue.put(values);
//                        }
//                    }
//                    values = reader.readNext();
//                   
//                }
//                while(!lsidQueue.isEmpty()){
//                    //logger.debug(">>>>>>The lsid queue has " + lsidQueue.size());
//                    try{
//                        Thread.currentThread().sleep(50);
//                    }
//                    catch(Exception e){
//                    
//                    }
//                }
//                //after each level has been processed commit the index
//                primaryThread.commit();              
//            }
//            catch(Exception e){
//                logger.error("Unable to reload " + geoLoads[0], e);
//            }
//        }  
    }
    
    private interface UpdateBiocache{
        
        public  List<SolrInputDocument> update(String[] values, boolean lookup, int id);
    }
    
    private class OccurrenceCountUpdate implements UpdateBiocache{
        private boolean index =true;
        OccurrenceCountUpdate(boolean index){
            this.index = index;
        }
        public List<SolrInputDocument> update(String[] value, boolean lookup, int id){
            
            try{                      
                String lsid = lookup?taxonConceptDao.findLsidByName(value[0]):value[0];
                if(lsid != null){
                    logger.debug(id+">>Indexing " + lsid);
                    Integer count= Integer.parseInt(value[1]);
                    logger.debug("Updating: " + value[0] +" : " + count);
                    taxonConceptDao.setOccurrenceRecordsCount(lsid, count);
                    if(index){
                        return taxonConceptDao.indexTaxonConcept(lsid,null);
                    }
                }
                else{
                    logger.warn("Unable to locate " + value);
                }
              }
              catch(Exception e){
                  logger.warn("Unable to generate index documents for " + value);
              }
            
            
            return null;
        }
    }
    
    
    private class GeospatialUpdate implements UpdateBiocache{
        String suffix = "http://biocache.ala.org.au/ws/occurrences/facets/download?q=lat_long:%5B*+TO+*%5D&count=true&facets=";
        private boolean index =true;
        GeospatialUpdate(boolean index){
            this.index = index;
        }
        public List<SolrInputDocument> update(String[] value, boolean lookup, int id){
            
            try{                      
                String lsid = lookup?taxonConceptDao.findLsidByName(value[0]):value[0];
                if(lsid != null){
                    logger.debug(id+">>Indexing " + lsid);
                    Integer count= Integer.parseInt(value[1]);
                    logger.debug("Updating: " + value[0] +" : " + count);
                    taxonConceptDao.setGeoreferencedRecordsCount(lsid, count);
                    if(index){
                        return taxonConceptDao.indexTaxonConcept(lsid,null);
                    }
                }
                else{
                    logger.warn("Unable to locate " + value);
                }
              }
              catch(Exception e){
                  logger.warn("Unable to generate index documents for " + value);
              }
            
            
            return null;
        }
    }
    
    private class ProcessingThread extends Thread{
        private BlockingQueue<String[]> facetQueue;
        private FileWriter writer;
        private int id;
        private int count;
        boolean running=true;
        
        ProcessingThread(BlockingQueue<String[]> queue,FileWriter writer, int num){
            logger.info("Creating thread " + num);
            this.facetQueue = queue;
            this.writer = writer;
            this.id = num;
            //this.query = query;
            
        }
        
        private void handleFacet(String query, String fq,String facet){
            logger.info("query : " + query + " fq=" + fq + " facet=" +facet);
            logger.info("Starting to load: Thread " + id + " " + query + " " +fq + " " + facet);
            boolean isGeoCount = !"*:*".equals(query);
            HttpClient httpClient = new HttpClient();
            String loadBase = facet.equals("taxon_concept_lsid") ? minorTaxonRanksUrlSuffix:suffix;
            String loadUrl = MessageFormat.format(loadBase, query, fq, facet);
            logger.info(id + " URL: " + loadUrl);
            GetMethod gm = new GetMethod(loadUrl);
            int coutn =0;
            try{
                logger.info(id + " Response code for get method: " +httpClient.executeMethod(gm));
                
                CSVReader reader = new CSVReader(new InputStreamReader(gm.getResponseBodyAsStream(), gm.getResponseCharSet()));
                String[] values = reader.readNext();
                logger.info("values: " + values.length);
                boolean lookup = !(values[0].contains("lsid") || values[0].contains("guid"));
                while(values!= null){
                  if(values.length == 2){
                      String lsid = lookup?taxonConceptDao.findLsidByName(values[0]):values[0];
                      count++;
                      if(lsid != null && lsid.length()>0 && !"Count".equals(values[1])){                            
                        //update it to
                        if(isGeoCount){
                            taxonConceptDao.setGeoreferencedRecordsCount(lsid, Integer.parseInt(values[1]));
//                          System.out.println("Geo Thread " + id + " " + facet + " is updating " + lsid + ": " + values[1]);
                        }
                        else{
                            //save the lsid to the file to reindex
                            synchronized(writer){
                                writer.write(lsid + "\n");
                            }
//                            System.out.println("Thread " + id + " " + facet + " is updating " + lsid + ": " + values[1]);
                            taxonConceptDao.setOccurrenceRecordsCount(lsid, Integer.parseInt(values[1]));
                        }                        
                        
                      }
                  }
                  values = reader.readNext();
                  if(count%1000 ==0 && count != 0)
                      logger.info("Thread " + id + " has loaded " + count + " at rank " + facet);
              }
                logger.info("Thread " + id + " has finished " + count + " at rank " + facet);
                writer.flush();
            }
            catch(Exception e){
                logger.error("Error in thread "+id + " query  :" + query + " facet : " + facet, e);
            }
        }
        
        public void run(){
            try{
              while (facetQueue.size() > 0 || running) {
                  if(facetQueue.size()>0){
                      String[] values = facetQueue.poll();                      
                      handleFacet(values[0], values[1], values[2]);
                  }
                  else{
                    logger.info(id + " is sleeping to wait for facet. " + facetQueue.size());
                    Thread.currentThread().sleep(100);
                  }
              }
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    

    /**
     * 
     * TODO move this out so it is more generic for other load purposes
     *
     */
    private class IndexingThread implements Runnable{
        private BlockingQueue<String[]> lsidQueue;
        private SolrServer solrServer = null;
        private List<SolrInputDocument> docs = null;
        private boolean isPrimary =false;
        private boolean lookup = false;
        private int id;
        private int count =0;
        private long lastStart = System.currentTimeMillis(); 
        IndexingThread(BlockingQueue<String[]> queue, List<SolrInputDocument> docs, int num){
            lsidQueue=queue;
            isPrimary = num==1;
            id = num;
            this.docs = docs;
            if(isPrimary){
                try{
                    solrServer = solrUtils.getSolrServer();
                }
                catch(Exception e){
                    
                }
            }
        }
        public void commit(){
            try{ 
                
                index();
                solrServer.commit();
                logger.info("Finished loading");
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        public void setLookup(boolean lu){
            this.lookup = lu;
        }
        private void index(){
            try{
                //logger.debug("sending " + docs.size() + " to the index");
                synchronized(docs){
                    if(docs.size() >0){
                        count+=docs.size();
                        logger.info("Adding items " + docs.size() + " to index");
                        solrServer.add(docs);
                    }
                    
                    long end = System.currentTimeMillis();
                logger.info(count
                        + " >> "
                        + ", records per sec: " + ((float)docs.size()) / (((float)(end - lastStart)) / 1000f)
                        + ", time taken for "+docs.size()+" records: " + ((float)(end - lastStart)) / 1000f
                        + ", total time: "+ ((float)(end - start)) / 60000f +" minutes");
                logger.info("");
                docs.clear();
                lastStart = System.currentTimeMillis();
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        public void run(){
            while(true){
                if(lsidQueue.size()>0){
                    String value[] = lsidQueue.poll();
                    try{                      
                      String lsid = lookup?taxonConceptDao.findLsidByName(value[0]):value[0];
                      if(lsid != null){
                          logger.debug(id+">>Indexing " + lsid);
                          Integer count= Integer.parseInt(value[1]);
                          logger.debug("Updating: " + value[0] +" : " + count);
                          taxonConceptDao.setGeoreferencedRecordsCount(lsid, count);
                          docs.addAll(taxonConceptDao.indexTaxonConcept(lsid,null));
                      }
                      else{
                          logger.warn("Unable to locate " + value);
                      }
                    }
                    catch(Exception e){
                        logger.warn("Unable to generate index documents for " + value);
                    }
                    if(docs.size()>=100 && isPrimary)
                        index();
                }
                else{
                    try{
                        Thread.currentThread().sleep(50);
                    }
                    catch(Exception e){}
                }
            }
        }
    }
    
}
