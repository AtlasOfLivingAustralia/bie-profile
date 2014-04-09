package org.ala.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.ala.dao.Scanner;
import org.ala.dao.StoreHelper;
import org.ala.model.Classification;
import org.ala.model.Image;
import org.ala.model.TaxonConcept;
import org.ala.model.Triple;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * 
 * Used to export the bie images metadata to csv.
 * 
 * Exports a file for each for each resource
 * Options to get the additional metadata from matched taxon or supplied classification
 * 
 * @author Natasha Quimby (natasha.quimby@csiro.au)
 *
 */
@Component("bieImageCSVExporter")
public class BieImageCSVExporter {
    @Inject
    protected StoreHelper storeHelper;
    /** The path to the repository */
    protected String repositoryPath = "/data/bie/";
    /** The URL to the repository */
    protected String repositoryUrl = "http://bie.ala.org.au/repo/";

    public static void main(String[] args) throws Exception {
        ApplicationContext context = SpringUtils.getContext();
        BieImageCSVExporter exporter = (BieImageCSVExporter)context.getBean(BieImageCSVExporter.class);
        //first arg is file location, second indicates local or web rdf
        //local rdf can only be performed from machines on which the rdf has been harvested
        String exportDir = "/data/bie/exports";
        boolean local =true;
        if(args.length==0){
            System.out.println("Exporting to default location " + exportDir +" using local rdf files");
        } 
        if(args.length >= 1){
            exportDir = args[0];
        }
        if(args.length>=2){
            local = args[1].equals("local");
        }
        System.out.println("Starting to export to " + exportDir + " using local rdf files: " + local);
        exporter.export(exportDir,"", local);
        System.exit(0);
    }
    /**
     * Exports all the images as a DWC CSV file.
     * @param baseDirectory The directory in which to export
     * @param startKey The start location for the cassandra scan.
     * @param localRdf Whether or not to use local rdf file. If not local a ws call will be performed - thus a slower export
     * @throws Exception
     */
    public void export(String baseDirectory,String startKey, boolean localRdf) throws Exception{
        FileUtils.forceMkdir(new File(baseDirectory));
        Scanner scanner =storeHelper.getScanner("tc", "tc", startKey, ColumnType.IMAGE_COL.getColumnName(), ColumnType.TAXONCONCEPT_COL.getColumnName(),ColumnType.CLASSIFICATION_COL.getColumnName());
        Map<String, List<String[]>> records = new HashMap<String, List<String[]>>();
        byte[] guidAsBytes = null;
        String[] header = new String[]{ "OccurrenceId","scientificName","kingdom","phylum","class","order","family","genus","specificEpithet","infraSpecificEpithet","rights","createdBy", "associatedMedia"};
        int i =0;
        while ((guidAsBytes = scanner.getNextGuid()) != null) {
            i++;
           List<Image> images =(List)scanner.getListValue(ColumnType.IMAGE_COL.getColumnName(), Image.class);
           TaxonConcept tc = (TaxonConcept)scanner.getValue(ColumnType.TAXONCONCEPT_COL.getColumnName(), TaxonConcept.class);
           List<Classification> cls = (List)scanner.getListValue(ColumnType.CLASSIFICATION_COL.getColumnName(), Classification.class);
           Classification cl = cls.size()>0 ?cls.get(0):null;
           boolean hasImage = false;
           if(cl!= null){
               for(Image image:images){
                   
                   String dataResourceUid = image.getInfoSourceUid();
                 //we don't want to export the biocache harvested images
                   if(image.getOccurrenceRowKey() == null && image.getOccurrenceUid() == null){                                             
                       if(dataResourceUid != null){
                         //get the list to add the record to
                           if(!records.containsKey(dataResourceUid)){
                               System.out.println("Adding " + dataResourceUid);
                               records.put(dataResourceUid, new java.util.ArrayList<String[]>());
                           }
                           List resourceList = records.get(dataResourceUid);
                           int lastFileSep = image.getRepoLocation().lastIndexOf(File.separatorChar);
                           if(lastFileSep < 0){
                               lastFileSep = image.getRepoLocation().lastIndexOf("/");
                           }
                           String baseUrl = image.getRepoLocation().substring(0, lastFileSep+1);
                           String[] rdfClass = localRdf ? getClassificationFromRDF(baseUrl):getClassificationFromRDFURL(baseUrl);
                           String[] row = (String[])ArrayUtils.addAll(ArrayUtils.add(rdfClass,0,image.getIdentifier()),
                                   new String[]{image.getRights(), image.getCreator(), fixRepoUrl(image.getRepoLocation())});
                                   
    //                               new String[]{image.getIdentifier(), tc.getNameString(),cl.getKingdom(),cl.getPhylum(),
    //                               cl.getClazz(), cl.getOrder(), cl.getFamily(), cl.getGenus(), cl.getSpecies(), cl.getSubspecies(), 
    //                               image.getRights(), image.getCreator(), fixRepoUrl(image.getRepoLocation())};
                           //System.out.println(StringUtils.join(row, ","));
                           //hasImage =true;
                           resourceList.add(row);
                           
                       }
                   }
               }
           }
           if(i%10000 ==0 ){
               System.out.println(new java.util.Date() +" - Processed " + i + " taxon concepts");
               
           }

        }
        System.out.println("Number of data resources " + records.size());
        for(String key : records.keySet()){
            List<String[]> values = records.get(key);
            System.out.println(key + " has " + values.size() + " images");
            writeResourceToFile(header, key, baseDirectory, values);
        }
        
    }
    /**
     * Writes the records for a data resource to the export location. Uses the data resource uid as the name of the file.
     * @param header The header for the CSV file
     * @param dataResource The data resource that the CSV file is for
     * @param baseDir The based directory in which to place the CSV file
     * @param values The values to write in the CSV file.
     * @throws Exception
     */
    private void writeResourceToFile(String[] header, String dataResource, String baseDir, List<String[]> values) throws Exception{
        CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(new File(baseDir + File.separator + dataResource+".csv")), Charset.forName("UTF-8")), ',','"','\\');
        try{
            writer.writeNext(header);
            writer.writeAll(values);
        } catch (Exception e){
            
        } finally {
            writer.flush();
            writer.close();
        }
    }
    /**
     * Retrieves the classification information from the local RDF files.
     * 
     * @param rdfLocation
     * @return an array that represents the classification of the record.
     */
    private String[] getClassificationFromRDF(String rdfLocation){
        String file = rdfLocation + File.separator +"rdf";
        try{
            List<Triple> triples = TurtleUtils.readTurtle(new FileReader(new File(file)));
            return getClassificationValuesFromTriples(triples);
        } catch (Exception e){
            System.err.println("Unable to read the RDF");
            e.printStackTrace();
        }
        return new String[]{"","","","","","","","",""};
    }
    /**
     * Extracts the scientific name details from the rdf that was originally supplied with the image via webservices.
     * @param rdfLocation
     * @return
     */
    private String[] getClassificationFromRDFURL(String rdfLocation){
        //String[] values = new String[]{};
        
        
        try{
            URL url = new URL(fixRepoUrl(rdfLocation) + "/rdf");
            List<Triple> triples=TurtleUtils.readTurtle(new BufferedReader(new InputStreamReader(url.openStream())));
            return getClassificationValuesFromTriples(triples);       
        } catch (Exception e){
            System.err.println("Unable to read the RDF");
            e.printStackTrace();
        }
        return new String[]{"","","","","","","","",""};
        
    }
    /**
     * Extracts the classification details from a list of triples.
     * @param triples
     * @return
     */
    private String[] getClassificationValuesFromTriples(List<Triple> triples){
        String kingdom="";
        String phylum="";
        String clazz="";
        String order="";
        String superfamily="";
        String family="";
        String genus="";
        String scientificName="";
        String species="";
        String specificEpithet="";
        String subspecies="";
        String infraspecificEpithet="";
        for(Triple triple:triples){
            String predicate = triple.predicate.substring(triple.predicate
                    .lastIndexOf("#") + 1);
            if (predicate.endsWith("hasKingdom")) {
                kingdom = triple.object.trim();
            }
            if (predicate.endsWith("hasPhylum")) {
                phylum = triple.object.trim();
            }
            if (predicate.endsWith("hasClass")) {
                clazz = triple.object.trim();
            }
            if (predicate.endsWith("hasOrder")) {
                order = triple.object.trim();
            }
            if (predicate.endsWith("hasFamily")) {
                family = triple.object.trim();
            }
            if (predicate.endsWith("hasSuperFamily")) {
                superfamily = triple.object.trim();
            }
            if (predicate.endsWith("hasGenus")) {
                genus = triple.object.trim();
            }
            if (predicate.endsWith("hasSpecies")) {
                species = (triple.object.trim());
            }
            if (predicate.endsWith("hasSubSpecies")) {
                subspecies = (triple.object.trim());
            }
            if (predicate.endsWith("hasSpecificEpithet")) {
                specificEpithet = triple.object.trim();
            }
            if (predicate.endsWith("hasScientificName")) {
                scientificName = (triple.object.trim());
            }
            if(scientificName.equals("")){
                //we need to make sure that we have items that can be used 
                if(genus.equals("") && specificEpithet.equals("")){
                    if(!subspecies.equals("")){
                        scientificName=subspecies;
                    } else if(!species.equals("")){
                        scientificName=species;
                    } else if(family.equals("") && !superfamily.equals("")){
                        scientificName=superfamily;
                    }
                } 
            }
            
        }
        String[] values = new String[]{scientificName,kingdom,phylum,clazz,order,family,genus,specificEpithet,infraspecificEpithet};
        return values;
    }
    
    /**
     * Turns a path into a repository URL that can be downloaded.
     * @param source
     * @return
     */
    private String fixRepoUrl(String source){
        if(source != null){
            return source.replaceFirst(repositoryPath, repositoryUrl);
        } else {
            return "";
        }
    }
    
}
