package org.ala.hbase;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.ala.dao.InfoSourceDAO;
import org.ala.dao.TaxonConceptDao;
import org.ala.model.CommonName;
import org.ala.model.ConservationStatus;
import org.ala.model.InfoSource;
import org.ala.util.SpringUtils;
import org.ala.vocabulary.Vocabulary;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.log4j.Logger;
import org.gbif.ecat.model.ParsedName;
import org.gbif.ecat.parser.NameParser;
import org.gbif.ecat.parser.UnparsableException;
import org.gbif.file.CSVReader;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import au.org.ala.data.model.LinnaeanRankClassification;

/**
 * Loads the conservation status and common names from the various conservation files
 * that are located in the /data/bie-staging/conservation directory.
 *
 * Each record will be matched to a BIE GUID using the ala_name_matching API.
 *
 * @author Natasha Carter
 */
@Component("conservationDataLoader")
public class ConservationDataLoader {

    protected static Logger logger = Logger.getLogger(ConservationDataLoader.class);
    @Inject
    protected InfoSourceDAO infoSourceDAO;
    @Inject
    protected Vocabulary vocabulary;
    @Inject
    protected TaxonConceptDao taxonConceptDao;
    protected Map<String, String[]> regionLookup;
    
    private Set<String> lsidsToReindex;
    
    private boolean statsOnly = false;
//   @Inject
    //protected DataSource gisDataSource;
//    /** JDBC Template for Postgres DB */
//    protected JdbcTemplate gisTemplate;
    private static final String epbcFile = "/data/bie-staging/conservation/epbc/EPBC_sprat.csv";
    private static final String qldFile = "/data/bie-staging/conservation/qld/QLD_DERM_WildNet_Conservation_Significant_Taxa_20120131.txt";
    private static final String waFaunaFile = "/data/bie-staging/conservation/wa/WA_FAUNA_LIST.csv";
    private static final String waFloraFile = "/data/bie-staging/conservation/wa/Flora201205.csv";
    private static final String saVertebratesFile = "/data/bie-staging/conservation/sa/vertebrates-bdbsa.csv";
    private static final String saVasculaFile = "/data/bie-staging/conservation/sa/vascula-plants-bdbsa.csv";
    private static final String vicDSEFile = "/data/bie-staging/conservation/vic/DSEAdvisory-VBA23-09-2010.csv";
    private static final String vicFFGFile = "/data/bie-staging/conservation/vic/FFGlisted-VBA23-09-2010.csv";
    private static final String nswCavsFile = "/data/bie-staging/conservation/nsw/CAVS.TXT";
    private static final String nswCapsFile = "/data/bie-staging/conservation/nsw/CAPS.TXT";
    private static final String nswFile = "/data/bie-staging/conservation/nsw/bionet20120514.csv";
    private static final String actFile = "/data/bie-staging/conservation/act/ACT_threatened_species.csv";
    private static final String ntFaunaEndangeredThreatFile = "/data/bie-staging/conservation/nt/NT_fauna.csv";
    private static final String ntFloraEndangeredThreatFile = "/data/bie-staging/conservation/nt/NT_flora.csv";
    private static final String ntPlantsNearThreatened ="/data/bie-staging/conservation/nt/NTPlantsNearThreatened.csv";
    private static final String ntAnimalsNearThreatened = "/data/bie-staging/conservation/nt/NTAnimalsNearTheatened.csv";
    private static final String iucnDirectory = "/data/bie-staging/conservation/iucn";
    private static final String iucnFile = "/data/bie-staging/conservation/iucn/2008_REDLIST.csv";
    private static final String tasFile = "/data/bie-staging/conservation/tas/species_2012.csv";
    private static final String summaryFile ="/data/bie-staging/conservation/stats/cons_name_matching_stats"+System.currentTimeMillis()+".csv";
    private static final String matchesFile = "/data/bie-staging/conservation/stats/matches"+System.currentTimeMillis()+".csv";
    private static final String reindexFile = "/data/bie-staging/conservation/reindex_lsids"+System.currentTimeMillis()+".txt";
    
    private static FileOutputStream summaryOut;
    private static FileOutputStream matchesOut;
    private FileOutputStream reindexOut;
    

    public static void main(String args[]) throws Exception{
        ApplicationContext context = SpringUtils.getContext();
        ConservationDataLoader loader = (ConservationDataLoader) context.getBean(ConservationDataLoader.class);
        
        if(args.length > 0)
            loader.statsOnly = args[0].equals("-stats");
        loader.init(context);
        List<String> infosources = Arrays.asList(args);//new HashSet<String>();
        loader.load(infosources);
    }

    public void load(List<String> infosources) {
        boolean all = infosources.contains("all");
        try {            
            if(all ||infosources.contains("500"))
                loadEpbc(',');
            if(all ||infosources.contains("506"))
                loadGenericStateOptionalClassification(nswFile, "New South Wales", "ALL", 506, 4, 6, 0, -1, 1, -1, 2, -1, 7,-1, 9);
            if(all || infosources.contains("501"))
                loadGenericStateOptionalClassification(qldFile,'\t',"Queensland","ALL",501,4,5,1,-1,2,-1,3,-1,8, 6,9);
            if(all || infosources.contains("509"))
                loadGenericStateOptionalClassification(tasFile, "Tasmania", "", 509, 0, 2, -1, -1, -1, -1, 3, -1, 5,1, 6);
            if(all || infosources.contains("505")){
                loadGenericState(vicDSEFile, "Victoria", "DSE Advisory", 505, 0, 1, 3);
                loadGenericState(vicFFGFile, "Victoria", "FFG Listed", 505, 0, 1, 4);
            }
            if(all || infosources.contains("507")){
                loadGenericState(actFile, "Australian Capital Territory", "Threatened", 507, 1, 0, 2);
            }
            if(all || infosources.contains("504")){
                loadGenericState(saVasculaFile, "South Australia", "Vascula Plants", 504, 6,2,7 );
                loadGenericState(saVertebratesFile, "South Australia",  "Vertebrates", 504, 4, 5, 6);
            }
            if(all || infosources.contains("508")){
                loadGenericState(ntFaunaEndangeredThreatFile, "Northern Territory", "Fauna", 508, 0, 2, 1);
                loadGenericState(ntFloraEndangeredThreatFile, "Northern Territory", "Flora", 508, 1, 2, 3);            
                loadGenericState(ntAnimalsNearThreatened, "Northern Territory", "Near Threatened Animals" , 508, 0, 1, 2);
                loadGenericState(ntPlantsNearThreatened, "Northern Territory", "Near Threatened Plants", 508, 0, -1, 1);
            }
            if(all || infosources.contains("503")){
                loadGenericState(waFloraFile, "Western Australia", "Flora Status", 503, 0, -1, 1);
                loadGenericState(waFloraFile, "Western Australia", "Flora Rank", 503, 0, -1, 2);
            }
            if(all || infosources.contains("502"))
                loadGenericState(waFaunaFile,"Western Australia", "Flora Status", 502, 3, 4, 6);
            if(all || infosources.contains("510"))    
                loadIUCN();
            
            //loader.loadWAFlora();
            //loader.loadWAFauna();
            //loader.loadGenericStateOptionalClassification(qldFile,'\t',"Queensland","ALL",501,4,5,1,-1,2,-1,3,-1,8,9);
            //loader.loadGenericStateOptionalClassification(tasFile, "Tasmania", "", 509, 0, 2, -1, -1, -1, -1, 3, -1, 5, 6);
            //loader.loadGenericState(vicDSEFile, "Victoria", "DSE Advisory", 505, 0, 1, 3);
            //loader.loadGenericState(vicFFGFile, "Victoria", "FFG Listed", 505, 0, 1, 4);
            //loader.loadGenericState(actFile, "Australian Capital Territory", "Threatened", 507, 1, 0, 2);
            //loader.loadGenericState(saVasculaFile, "South Australia", "Vascula Plants", 504, 6,2,7 );
            //loader.loadGenericState(saVertebratesFile, "South Australia",  "Vertebrates", 504, 4, 5, 6);
            //loader.loadGenericState(ntFaunaEndangeredThreatFile, "Northern Territory", "Fauna", 508, 0, 2, 1);
            //loader.loadGenericState(ntFloraEndangeredThreatFile, "Northern Territory", "Flora", 508, 1, 2, 3);
            //loader.loadGenericState(ntPlantsNearThreatened, "Northern Territory", "Near Threatened Plants", 508, 0, -1, 1);
//            loader.loadGenericState(ntAnimalsNearThreatened, "Northern Territory", "Near Threatened Animals" , 508, 0, 1, 2);
            
//            loader.loadEpbc();
//            loader.loadQueensland();
//            loader.loadWAFauna();
//            loader.loadWAFlora();
//            //load the SA files using the generic load method
//            loader.loadGenericState(saVertebratesFile, "South Australia",  "Vertebrates", 504, 5, 6, 7);
//            loader.loadGenericState(saVasculaFile, "South Australia", "Vascula Plants", 504, 4,5,6 );
//            //load the Vic files using the generic load method
//            loader.loadGenericState(vicDSEFile, "Victoria", "DSE Advisory", 505, 0, 1, 3);
//            loader.loadGenericState(vicFFGFile, "Victoria", "FFG Listed", 505, 0, 1, 4);
//            //load NSW (has higher level classifications)
//            loader.loadNSW(nswCavsFile, "CAVS", 6, 1, 3, 7, 8);
//            loader.loadNSW(nswCapsFile, "CAPS", 7, 1, 3, 9, 10);
//            //load ACT using the generic load method
//            loader.loadGenericState(actFile, "Australian Capital Territory", "Threatened", 507, 1, 0, 2);
//            //load NT using generic
//            loader.loadGenericState(ntFaunaEndangeredThreatFile, "Northern Territory", "Fauna", 508, 0, 2, 1);
//            loader.loadGenericState(ntFloraEndangeredThreatFile, "Northern Territory", "Flora", 508, 1, 2, 3);
//            loader.loadGenericState(ntPlantsNearThreatened, "Northern Territory", "Near Threatened Plants", 508, 0, -1, 1);
//            loader.loadGenericState(ntAnimalsNearThreatened, "Northern Territory", "Near Threatened Animals" , 508, 0, 1, 2);
//            loader.loadIUCN();
////            loader.processIUCN();
//            loader.loadGenericStateOptionalClassification(tasFile, "Tasmania", "", 509,4 , 23, 5, 6, 7, 8, 9, 10, 3, 24);
            
            //now write the set to file
            for(String lsid : lsidsToReindex)
                reindexOut.write((lsid+"\n").getBytes());
            reindexOut.flush();
            reindexOut.close();
            summaryOut.flush();
            summaryOut.close();
            matchesOut.flush();
            matchesOut.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public void init(ApplicationContext context) throws Exception {
        reindexOut = FileUtils.openOutputStream(new File(reindexFile));
        summaryOut = FileUtils.openOutputStream(new File(summaryFile));
        summaryOut.write("source,filename,ANBG matched,Other match,No Match,Homonyms detected\n".getBytes());
        matchesOut = FileUtils.openOutputStream(new File(matchesFile));
        matchesOut.write("file,scientific name,matched\n".getBytes());
        
        lsidsToReindex = new HashSet<String>();
        //populate the region lookup for use in the EPBC infosource
        //This should be change to use the Gazetteer 
        regionLookup = new HashMap<String, String[]>();
        regionLookup.put("ACT", new String[]{"Australian Capital Territory", "aus_states/Australian Capital Territory"});
        regionLookup.put("QLD", new String[]{"Queensland", "aus_states/Queensland"});
        regionLookup.put("TAS", new String[]{"Tasmania", "aus_states/Tasmania"});
        regionLookup.put("VIC", new String[]{"Victoria", "aus_states/Victoria"});
        regionLookup.put("NSW", new String[]{"New South Wales", "aus_states/New South Wales"});
        regionLookup.put("WA", new String[]{"Western Australia", "aus_states/Western Australia"});
        regionLookup.put("NT", new String[]{"Northern Territory", "aus_states/Northern Territory"});
        regionLookup.put("SA", new String[]{"South Australia", "aus_states/South Australia"});
        regionLookup.put("Norfolk Island", new String[]{"Norfolk Island Province", "imcra/Norfolk Island Province"});
        regionLookup.put("Cocos Island", new String[]{"Cocos (Keeling) Island Province", "imcra/Cocos (Keeling) Island Province"});
        regionLookup.put("Christmas Island", new String[]{"Christmas Island Province", "imcraChristmas Island Province"});
        regionLookup.put("Kangaroo Island", new String[]{"Kangaroo Island", "gadm_admin/Kangaroo Island"});
        regionLookup.put("Lord Howe Island", new String[]{"Lord Howe Province", "imcra/Lord Howe Province"});
        regionLookup.put("Macquarie Island Province", new String[]{"Macquarie Island Province", "imcra/Macquarie Island Province"});

//        gisDataSource = (DataSource) context.getBean("gisDataSource");
//        gisTemplate = new JdbcTemplate(gisDataSource);
        //TODO Fix up how this is obtaining the region information

    }
    private void writeMatch(String source,String name, String lsid) throws Exception{
        writeMatch(source, name,lsid, "");
    }
    private void writeMatch(String source, String name, String lsid, String kingdom) throws Exception{
        matchesOut.write(("\"" + source + "\",\"" + name + "\",\"" + lsid+ "\",\"" + kingdom + "\"\n").getBytes());
    }

    private String[] getRegionInfo(String region) {
        return regionLookup.get(region);
    }
//    private void reportSummary(String file, String filename, int total, int anbg, int col, int failed) throws Exception{
//        String line = file + "," +filename + "," + total+","+anbg+"," +col + "," + failed+"\n";
//        summaryOut.write(line.getBytes());
//    }

    /**
     * Load the content of the EPBC file
     * @throws Exception
     */
    public void loadEpbc(char separator) throws Exception {
        logger.info("Starting to load EPBC...");
        CSVReader reader = CSVReader.buildReader(new File(epbcFile), "UTF-8", separator, '"', 1);
        NameParser parser = new NameParser();
        Pattern p = Pattern.compile(",");
        InfoSource is = infoSourceDAO.getById(500);
        int processed = 0, failed = 0, anbg=0, col=0;
        taxonConceptDao.resetStats();
        while (reader.hasNext()) {
            String values[] = reader.readNext();
            if (values != null && values.length > 10) {
                String speciesName = values[0];
                ParsedName pn = null;
                try{        
                pn = parser.parse(speciesName);
                }
                catch(UnparsableException e){
                    logger.warn(e.getMessage());
                }
                String genus = pn == null ? null : pn.getGenusOrAbove();
                LinnaeanRankClassification cl = new LinnaeanRankClassification(values[6], values[7], values[8], values[9], values[10], genus, speciesName);
                String guid = taxonConceptDao.findLsidByName(values[0], cl, null, null,is, true,true);
                if (guid != null) {
                    if(!lsidsToReindex.contains(guid))
                        lsidsToReindex.add(guid);
                    else{
                        if(guid.startsWith("ALA"))
                            logger.warn("EPBC ALREADY ADDED " + guid);
                    }
                    writeMatch("EPBC", values[0], guid, values[6]);
                    processed++;
                    if(guid.startsWith("urn:lsid:biodiversity.org.au"))
                        anbg++;
                    else
                        col++;
                    if(!statsOnly){
                        //BIE has the supplied species names
                        String commonNames = StringUtils.trimToNull(values[1]);
                        String conservationStatus = values[2];
                        String comments = values[4];
                        ConservationStatus cs = vocabulary.getConservationStatusFor(500, conservationStatus);
                        //add the info source information
                        addCSInfo(cs, is, "Australia", null);//This is the national list

                        //add the regions to which the status applies
                        String regions = values[5];
                        //multiple regions are separated by ,
                        if (regions != null) {
                            for (String region : p.split(regions)) {
                                String[] rinfo = getRegionInfo(StringUtils.trim(region));
                                if (rinfo == null) {
                                    //attempt to get the region from the comments field
                                    rinfo = getRegionInfo(comments);
                                }
                                if (rinfo != null) {
                                    cs.addOtherRegion(rinfo[0], rinfo[1]);
                                }
                            }
                        }

                        taxonConceptDao.addConservationStatus(guid, cs);
                        //add the common names(s)
                        if (commonNames != null) {
                            //multiple common names are split by a comma
                            for (String cn : p.split(commonNames)) {
                                addCommonName(guid, cn, is);
                            }

                        }
                    }


                    //System.out.println("The Conservation Status: " + cs);
                } else {
                    failed++;
                    logger.info("Unable to locate " + speciesName + ","+values[6]);
                }
            }
        }
        logger.info("Finished adding " + processed + " conservation status.  Failed to locate " + failed);
        taxonConceptDao.reportStats(summaryOut, "EPBC,"+ epbcFile);
        //reportSummary("EPBC",epbcFile, processed+failed, anbg, col, failed);
    }

    private void addCSInfo(ConservationStatus cs, InfoSource is, String region, String regionId) {
        cs.setInfoSourceId(Integer.toString(is.getId()));
        cs.setInfoSourceName(is.getName());
        cs.setInfoSourceURL(is.getWebsiteUrl());
        cs.setInfoSourceUid(is.getUid());
        cs.setRegion(region);
        cs.setRegionId(regionId);
    }

    /**
     * Adds a new common name to the specified taxon concept.
     * @param guid
     * @param cn
     * @param is
     * @throws Exception
     */
    private void addCommonName(String guid, String cn, InfoSource is) throws Exception {
        CommonName commonName = new CommonName();
        commonName.setNameString(WordUtils.capitalizeFully(cn));
        if(is != null){
            commonName.setInfoSourceId(Integer.toString(is.getId()));
            commonName.setInfoSourceName(is.getName());
            commonName.setInfoSourceURL(is.getWebsiteUrl());
        }
        //System.out.println("Common Name: " + commonName);
        taxonConceptDao.addCommonName(guid, commonName);
    }

    /**
     * loads the Queensland conservation status
     * @throws Exception
     */
    public void loadQueensland() throws Exception {
        logger.info("Starting to load Queensland ...");
        CSVReader reader = CSVReader.buildReader(new File(qldFile), "UTF-8", ',', '"', 1);
        InfoSource is = infoSourceDAO.getById(501);

        int processed = 0, failed = 0, anbg=0, col =0;
        taxonConceptDao.resetStats();
        while (reader.hasNext()) {
            String values[] = reader.readNext();
            if (values != null && values.length > 8) {
                String sciName = values[4];
                //In this file the kingdom and class are common names thus
                //initially we are only going to search by scientific name
                //We may need to change this if there are homonyms...
                String guid = taxonConceptDao.findLsidByName(sciName, true);
                if (guid != null) {
                    writeMatch("QLD", sciName, guid);
                    processed++;
                    if(guid.startsWith("urn:lsid:biodiversity.org.au"))
                        anbg++;
                    else
                        col++;
                    if(!statsOnly){
                        ConservationStatus cs = vocabulary.getConservationStatusFor(501, values[7]);
                        //some of the species do not have a qld conservation status
                        if (cs != null) {

                            //System.out.println("The qld conservation status = " + cs);
                            //set the region and region id  (all records will be the same thus not in the vocabulary)
                            addCSInfo(cs, is, "Queensland", "aus_states/Queensland");
                            taxonConceptDao.addConservationStatus(guid, cs);
                        }
                        //add the common name
                        String commonName = StringUtils.trimToNull(values[5]);
                        if (commonName != null) {
                            addCommonName(guid, commonName, is);
                        }
                    }
                } else {
                    failed++;
                    logger.info("Unable to locate scientific name " + sciName);
                }

            }
        }
        logger.info("Finished adding " + processed + " conservation status.  Failed to locate " + failed);
        //reportSummary("Queensland",qldFile, processed+failed, anbg, col, failed);
        taxonConceptDao.reportStats(summaryOut, "Queensland,"+ qldFile);

    }

    /**
     * loads the WA Fauna file (including common names)
     * @throws Exception
     */
    public void loadWAFauna() throws Exception {
        logger.info("Loading the Western Australian Fauna Conservation Status ...");
        CSVReader reader = CSVReader.buildReader(new File(waFaunaFile), "UTF-8", ',', '"', 1);
        InfoSource is = infoSourceDAO.getById(502);
        NameParser parser = new NameParser();
        int processed = 0, failed = 0, anbg=0, col =0;
        taxonConceptDao.resetStats();

        while (reader.hasNext()) {
            String[] values = reader.readNext();
            if (values != null && values.length > 8) {
                String sciName = values[3];
                ParsedName pn = null;
                try{
                pn = parser.parse(sciName);
                }
                catch(UnparsableException e){
                    logger.warn(e.getMessage());
                }
                String genus = pn == null ? null : pn.getGenusOrAbove();
                //fauna should be animalia
                LinnaeanRankClassification cl = new LinnaeanRankClassification("Animalia", genus);
                //family is the only non-vernacular name rank available
                cl.setFamily(values[8]);
                String guid = taxonConceptDao.findLsidByName(sciName, cl, null,true);
                if (guid != null) {
                    writeMatch("WA Fauna", sciName, guid);
                    processed++;
                    if(guid.startsWith("urn:lsid:biodiversity.org.au"))
                        anbg++;
                    else
                        col++;
                    if(!statsOnly){
                        //get the conservation status
                        ConservationStatus cs = vocabulary.getConservationStatusFor(502, values[6]);
                        if (cs != null) {
                            addCSInfo(cs, is, "Western Australia", "aus_states/Western Australia");
    //                        System.out.println("Adding " + cs);
                            taxonConceptDao.addConservationStatus(guid, cs);
                        }
                        //add the common names
                        String commonName = StringUtils.trimToNull(values[4]);
                        if (commonName != null) {
                            addCommonName(guid, commonName, is);
                        }
                    }

                } else {
                    logger.info("Unable to locate scientific name " + sciName);
                    failed++;
                }

            }

        }

        logger.info("Finished adding " + processed + " conservation status.  Failed to locate " + failed);
        //reportSummary("WA Fauna",waFaunaFile, processed+failed, anbg, col, failed);
        taxonConceptDao.reportStats(summaryOut,"WA Fauna,"+waFaunaFile);
    }

    /**
     * Loads the Western Australian flora conservation lists.
     *
     * The WA flora list has 2 different columns that could supply the conservation status
     *
     * @throws Exception
     */
    public void loadWAFlora() throws Exception {
        logger.info("Loading the Western Australian Flora Conservation Status ...");
        CSVReader reader = CSVReader.buildReader(new File(waFloraFile), "UTF-8", ',', '"', 1);
        InfoSource is = infoSourceDAO.getById(503);

        int processed = 0, failed = 0, loaded = 0, anbg=0, col=0;
        taxonConceptDao.resetStats();
        while (reader.hasNext()) {
            String[] values = reader.readNext();
            if (values != null && values.length >= 3 ){//&& StringUtils.isNotBlank(values[2])) {
                String sciName = values[0];
                String guid = taxonConceptDao.findLsidByName(sciName,true);
                if (guid != null) {
                    writeMatch("WA Flora", sciName, guid);
                    processed++;
                    if(guid.startsWith("urn:lsid:biodiversity.org.au"))
                        anbg++;
                    else
                        col++;
                    if(!statsOnly){
                        //check for the conservation status of the WA IUCN Rank column
                        // This will process the cons code = X and cons code = R to more detailed conservation codes.
                        ConservationStatus cs = vocabulary.getConservationStatusFor(503, values[2]);
                        if (cs == null) {
                            //Now check for the conservation status of the Cons code column.
                            // This will catch the records that have conservation status 1, 2, 3 or 4
                            cs = vocabulary.getConservationStatusFor(503, values[1]);
                        }
                        if (cs != null) {
                            loaded++;
                            addCSInfo(cs, is, "Western Australia", "aus_states/Western Australia");
    //                        System.out.println(cs);
                            taxonConceptDao.addConservationStatus(guid, cs);
                        }
                        //there are no common names in the wa flora file.
                    }
                } else {
                    failed++;
                    logger.info("Unable to locate scientific name " + sciName);
                }
            }
        }
        logger.info("Finished adding " + processed + "(" + loaded + ") conservation status.  Failed to locate " + failed);
        //reportSummary("WA Flora",waFloraFile, processed+failed, anbg, col, failed);
        taxonConceptDao.reportStats(summaryOut, "WA Flora,"+waFloraFile);
    }

    public void loadNSW(String filename, String type, int sciIdx, int familyIdx, int genusId, int cnIdx, int statusIdx) throws Exception {
        logger.info("Loading the NSW " + type + " Conservation Status ...");
        CSVReader reader = CSVReader.buildReader(new File(filename), "UTF-8", '\t', '\"', 1);
        InfoSource is = infoSourceDAO.getById(506);
        int processed = 0, failed = 0, loaded = 0, anbg=0 ,col=0;
        taxonConceptDao.resetStats();
        while (reader.hasNext()) {
            String[] values = reader.readNext();
            if (values != null && values.length > statusIdx) {
                String sciName = values[sciIdx];
                LinnaeanRankClassification cl = new LinnaeanRankClassification(null, null, null, null, values[familyIdx], values[genusId], sciName);
                try{
                String guid = taxonConceptDao.findLsidByName(sciName, cl, null, true);
                if (guid != null) {
                    writeMatch("NSW " +type, sciName, guid);
                    processed++;
                     if(guid.startsWith("urn:lsid:biodiversity.org.au"))
                        anbg++;
                    else
                        col++;
                    if(!statsOnly){
                        ConservationStatus cs = vocabulary.getConservationStatusFor(506, values[statusIdx]);
                        if (cs != null) {
                            loaded++;
                            addCSInfo(cs, is, "New South Wales", "aus_states/New South Wales");
                            taxonConceptDao.addConservationStatus(guid, cs);
                        }
                        //common name
                        String commonName = StringUtils.trimToNull(values[cnIdx]);
                        if (commonName != null) {
                            addCommonName(guid, commonName, is);
                        }
                    }
                } else {
                    failed++;
                    logger.info("Unable to locate scientific name " + sciName + " [" + (processed + failed) + "]");
                }
                }
                catch(Exception e){
                    e.printStackTrace();
                    System.out.println("PROBLEM RECORD " + StringUtils.join(values, ","));
                }


            }
        }
        logger.info("Finished adding " + processed + "(" + loaded + ") conservation status.  Failed to locate " + failed);
        //reportSummary("NSW " + type, filename, processed+failed, anbg, col, failed);
        taxonConceptDao.reportStats(summaryOut, "NSW " + type+","+ filename);
    }

    
    public void loadGenericStateOptionalClassification(String filename, String state, String type, int infosourceId, int sciIdx, int cnIdx, int kidx, int pidx, int cidx, int oidx, int fidx, int gidx, int statusIdx, int authorityIdx, int minCols) throws Exception{
        loadGenericStateOptionalClassification(filename, ',', state, type, infosourceId, sciIdx, cnIdx, kidx, pidx, cidx, oidx, fidx, gidx, statusIdx, authorityIdx, minCols);
    }
    
    /**
     * Generically loads a state that minimally contains a kingdom and genus 
     * @param filename
     * @param state
     * @param type
     * @param infosourceId
     * @param sciIdx
     * @param cnIdx
     * @param kidx
     * @param pidx
     * @param cidx
     * @param oidx
     * @param fidx
     * @param gidx
     * @param statusIdx
     * @param minCols
     * @throws Exception
     */
    public void loadGenericStateOptionalClassification(String filename,char separator, String state, String type, int infosourceId, int sciIdx, int cnIdx, int kidx, int pidx, int cidx, int oidx, int fidx, int gidx, int statusIdx, int authorityIdx, int minCols) throws Exception{
        logger.info("Loading the " + state + " " + type + " Conservation Status...");
        CSVReader reader = CSVReader.buildReader(new File(filename), "UTF-8", separator, '"', 1);
        InfoSource is = infoSourceDAO.getById(infosourceId);
        int processed = 0, failed = 0, loaded = 0, anbg=0, col=0;
        taxonConceptDao.resetStats();

        while (reader.hasNext()) {
            String[] values = reader.readNext();
            if (values != null && values.length >= minCols&&StringUtils.isNotBlank(values[statusIdx])) {
                String sciName = values[sciIdx];
                String genus = (gidx>0)?values[gidx]:null;
                String kingdom = (kidx >0)?values[kidx]:null;
                //get the classification
                LinnaeanRankClassification cl = new LinnaeanRankClassification(kingdom, genus);
                //Add the optional ranks to the classification
                if(pidx >=0)
                    cl.setPhylum(values[pidx]);
                if(cidx >=0)
                    cl.setKlass(values[cidx]);
                if(oidx >= 0)
                    cl.setOrder(values[oidx]);
                if(fidx >= 0)
                    cl.setFamily(values[fidx]);
                //get the guid for the species
                String authority =authorityIdx>=0?values[authorityIdx]:null;
                String guid = taxonConceptDao.findLsidByName(sciName, cl, null,authority,is,true,true);
                if (guid != null) {
                    if(!lsidsToReindex.contains(guid))
                        lsidsToReindex.add(guid);
                    else{
                        if(guid.startsWith("ALA"))
                            logger.warn(state + " " + type+" ALREADY ADDED " + guid);
                    }
                    writeMatch(state + " " + type, sciName, guid);
                    processed++;
                     if(guid.startsWith("urn:lsid:biodiversity.org.au"))
                        anbg++;
                    else
                        col++;
                    if(!statsOnly){
                        //get the conservation status
                        ConservationStatus cs = vocabulary.getConservationStatusFor(infosourceId, values[statusIdx]);
                        if (cs != null) {
                            loaded++;
                            addCSInfo(cs, is, state, "aus_states/" + state);
    //                        System.out.println(cs);
                            taxonConceptDao.addConservationStatus(guid, cs);
                        }
                        else{
                            logger.warn("Unable to find supplied conservation status " + values[statusIdx] + " for " +sciName);
                        }
                        if (cnIdx >= 0) {
                            //now load the common names
                            String commonName = StringUtils.trimToNull(values[cnIdx]);
                            if (commonName != null) {
                                addCommonName(guid, commonName, is);
                            }
                        }
                    }
                } else {
                    failed++;
                    logger.info("Unable to locate scientific name " + sciName);
                }
            }
        }
        logger.info("Finished adding " + processed + "(" + loaded + ") conservation status.  Failed to locate " + failed);
        //reportSummary(state + " " + type, filename, processed+failed, anbg, col, failed);
        taxonConceptDao.reportStats(summaryOut, state + " " + type+","+ filename);
        taxonConceptDao.refreshNameIndex();
    }

    /**
     * Generically loads a states conservation status.
     *
     * This can only be used in situations where no extra level of classification
     * is being added to the name search. Also where not additional processing is being performed.
     * 
     * @param filename
     * @param type
     * @param sciIdx The index of the scientific name
     * @param cnIdx  The index of the common name. Wen negative no common name is checked
     * @param statusIdx  The index of the conservation status
     * @param system The system that conservation status is part of
     * @throws Exception
     */
    public void loadGenericState(String filename, String state, String type, int infosourceId, int sciIdx, int cnIdx, int statusIdx) throws Exception {
        logger.info("Loading the " + state + " " + type + " Conservation Status...");
        loadGenericStateOptionalClassification(filename, ',', state, type, infosourceId, sciIdx, cnIdx, -1, -1, -1, -1, -1, -1, statusIdx, -1, statusIdx);
//        CSVReader reader = CSVReader.buildReader(new File(filename), "UTF-8", ',', '"', 1);
//        InfoSource is = infoSourceDAO.getById(infosourceId);
//        int processed = 0, failed = 0, loaded = 0, anbg = 0, col = 0;
//        taxonConceptDao.resetStats();
//
//        while (reader.hasNext()) {
//            String[] values = reader.readNext();
//            if (values != null && values.length > statusIdx) {
//                String sciName = values[sciIdx];
//                //get the guid for the species
//                String guid = taxonConceptDao.findLsidByName(sciName,true);
//                if (guid != null) {
//                    writeMatch(state + " "+ type, sciName, guid);
//                    processed++;
//                     if(guid.startsWith("urn:lsid:biodiversity.org.au"))
//                        anbg++;
//                    else
//                        col++;
//                    if(!statsOnly){
//                        //get the conservation status
//                        ConservationStatus cs = vocabulary.getConservationStatusFor(infosourceId, values[statusIdx]);
//                        if (cs != null) {
//                            loaded++;
//                            addCSInfo(cs, is, state, "aus_states/" + state);
//    //                        System.out.println(cs);
//                            taxonConceptDao.addConservationStatus(guid, cs);
//                        }
//                        if (cnIdx >= 0) {
//                            //now load the common names
//                            String commonName = StringUtils.trimToNull(values[cnIdx]);
//                            if (commonName != null) {
//                                addCommonName(guid, commonName, is);
//                            }
//                        }
//                    }
//                } else {
//                    failed++;
//                    logger.info("Unable to locate scientific name " + sciName);
//                }
//            }
//        }
//        logger.info("Finished adding " + processed + "(" + loaded + ") conservation status.  Failed to locate " + failed);
//        //reportSummary(state+" " + type,filename, processed+failed, anbg, col, failed);
//        taxonConceptDao.reportStats(summaryOut, state+" " + type+","+filename);
    }

    private void loadIUCN() throws Exception {
        logger.info("Loading the IUCN Redlist...");
        CSVReader reader = CSVReader.buildReader(new File(iucnFile), "UTF-8", ',', '"', 1);
        InfoSource is = infoSourceDAO.getById(510);
        int processed = 0, failed = 0, anbg=0, col=0;
        taxonConceptDao.resetStats();

        while(reader.hasNext()){
            String[] values = reader.readNext();
            if(values != null && values.length >= 24){
                //check to see if we want to load this record
                //We are only loading records that are not related to a specific population
                String pop = StringUtils.trimToNull(values[12]);
                String sciName = values[23];
                if(pop == null){
                    
                    String k = StringUtils.capitalize(values[1].toLowerCase());
                    String p = StringUtils.capitalize(values[2].toLowerCase());
                    String c = StringUtils.capitalize(values[3].toLowerCase());
                    String o = StringUtils.capitalize(values[4].toLowerCase());
                    String f = StringUtils.capitalize(values[5].toLowerCase());
                    String g = values[6];
                    LinnaeanRankClassification cl = new LinnaeanRankClassification(k,p,c,o,f,g,sciName);
                    String guid = taxonConceptDao.findLsidByName(sciName, cl, null,true); //don't add the missing species because this is an international list

                    if(guid != null){
                        if(!lsidsToReindex.contains(guid))
                            lsidsToReindex.add(guid);
                        else{
                            if(guid.startsWith("ALA"))
                                logger.warn("IUCN ALREADY ADDED " + guid);
                        }
                        writeMatch("IUCN", sciName, guid);
                        processed++;
                         if(guid.startsWith("urn:lsid:biodiversity.org.au"))
                        anbg++;
                    else
                        col++;
                        if(!statsOnly){
                            ConservationStatus cs = vocabulary.getConservationStatusFor(510, values[17]);
                            addCSInfo(cs, is, null, null);
                            taxonConceptDao.addConservationStatus(guid, cs);

                            String commonNameString = StringUtils.trimToNull(values[14]);
                            if(commonNameString != null){
                            	String[] commonNames = commonNameString.split(",");
                            	for(String commonName: commonNames){
                            		addCommonName(guid, commonName, is);
                            	}
                            }
                        }
                    }
                    else{
                        failed++;
                        logger.info("Unable to locate scientific name " + sciName);
                    }
                }
                else{
                    System.out.println("Will not process " + sciName + " population " + pop);
                }
            }
        }
        logger.info("Finished adding " + processed +" conservation status.  Failed to locate " + failed);
        //reportSummary("IUCN",iucnFile, processed+failed, anbg, col, failed);
        taxonConceptDao.reportStats(summaryOut, "IUCN,"+iucnFile);
    }

//    private void processIUCN() throws Exception{
//        //the IUCN is in the form of a DWC Archive.
//        Archive archive = ArchiveFactory.openArchive(new File(iucnDirectory),true);
//        Iterator<DarwinCoreRecord> iter = archive.iteratorDwc();
//        while(iter.hasNext()){
//            DarwinCoreRecord dwcr = iter.next();
//            if(dwcr!= null){
//                //get the scietific name
//                String name = dwcr.getScientificName();
//                LinnaeanRankClassification cl = new LinnaeanRankClassification(dwcr.getKingdom(), dwcr.getPhylum(), dwcr.getClasss(), dwcr.getOrder(),dwcr.getFamily(), dwcr.getGenus(), name);
//                String guid = taxonConceptDao.findLsidByName(name, cl, null);
//                if(guid != null){
//                    //we have found a record that needs to be processed
//
//                    String status = StringUtils.trimToNull(dwcr.getProperty(GbifTerm.threatStatus));
//                    if(status != null){
//                        System.out.println(dwcr.getIdentificationID() + " has scientific name " + name + " status : " + status);
//                    }
//
//                    //only process the English common names
//                    String commonName = StringUtils.trimToNull(dwcr.getVernacularName());
//                    String language = StringUtils.trimToNull(dwcr.getLanguage());
//                    if(commonName!=null && language != null){
//                        if(language.equalsIgnoreCase("en")){
//                            System.out.println(dwcr.getIdentificationID() + " " + name + " has common name: "+ commonName);
////                            addCommonName(guid, commonName, is);
//                        }
//                    }
//                }
//
//            }
//        }
//    }

    public void setTaxonConceptDao(TaxonConceptDao taxonConceptDao) {

        this.taxonConceptDao = taxonConceptDao;
    }

    public void setInfoSourceDAO(InfoSourceDAO infoSourceDAO) {
        this.infoSourceDAO = infoSourceDAO;
    }

    /**
     * @param taxonConceptDao the taxonConceptDao to set
     */
    public void setVocabulary(Vocabulary vocabulary) {
        this.vocabulary = vocabulary;
    }
}
