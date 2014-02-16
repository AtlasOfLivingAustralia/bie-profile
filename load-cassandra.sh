start_time=$(date +%s)

echo "LOAD : running processing $('date')"

mvn clean package install -DskipTests=true

cd target

jar xf bie-profile-assembly.jar lib lib

export CLASSPATH=.:bie-profile-assembly.jar

# echo "LOAD : loading the geographic regions into the BIE $('date')"
# java -classpath $CLASSPATH org.ala.hbase.GeoRegionLoader

echo "LOAD : creating lucene indexes for concept lookups $('date')"
java -Xmx2g -Xms2g -classpath $CLASSPATH au.org.ala.checklist.lucene.CBCreateLuceneIndex /data/bie-staging/ala-names /data/lucene/namematching13

echo "LOAD : creating a symbolic link for the newly created index $('date')"
ln -s /data/lucene/namematching13 /data/lucene/namematching

echo "LOAD : running alanames  data load $('date')"
java -Xmx1g -Xms1g -classpath $CLASSPATH org.ala.hbase.ALANamesLoader

echo "LOAD : creating loading indicies $('date')"
java -classpath $CLASSPATH org.ala.lucene.CreateLoadingIndex

echo "LOAD : running ANBG data load $('date')"
java -classpath $CLASSPATH org.ala.hbase.ANBGDataLoader

echo "LOAD : running DWCA data load $('date')"
java -classpath $CLASSPATH org.ala.hbase.DwCALoader /data/bie-staging/names-lists/dwca_ausmoss_20140117_1607/
java -classpath $CLASSPATH org.ala.hbase.DwCALoader /data/bie-staging/names-lists/dwca_ausfungi_20140124_1323/

echo "LOAD : loading the Conservation codes into the BIE $('date')"
java -classpath $CLASSPATH org.ala.hbase.ConservationDataLoader all

echo "LOAD : running Col Names Processing $('date')"
java -classpath $CLASSPATH org.ala.preprocess.ColFamilyNamesProcessor

echo "LOAD : running Repository Data Loader $('date')"
java -Xmx1g -Xms1g -classpath $CLASSPATH org.ala.hbase.RepoDataLoader

# echo "LOAD : running Bio Cache Loader $('date')"
# java -Xmx2g -Xms2g -classpath $CLASSPATH org.ala.hbase.BioCacheLoader

echo "LOAD : running Irmng Loader $('date')"
java -classpath $CLASSPATH org.ala.hbase.IrmngDataLoader

# echo "LOAD : running BHL Data Loader $('date')"
# java -classpath $CLASSPATH org.ala.hbase.BHLDataLoader

# echo "LOAD : loading the geographic region emblems into the BIE $('date')"
# java -classpath $CLASSPATH org.ala.hbase.EmblemLoader

echo "LOAD : loading the iconic species into the BIE $('date')"
java -classpath $CLASSPATH org.ala.hbase.IconicSpeciesLoader

# This one can't be run until biocache has been released with new names.
# echo "LOAD : loading the Australian taxon concepts into the BIE $('date')"
# java -classpath $CLASSPATH org.ala.hbase.AustralianTaxonLoader

echo "LOAD : loading the Standard Common Names into the BIE $('date')"
java -classpath $CLASSPATH org.ala.hbase.StandardNameLoader -all

# Can't be ran until biocache has been loaded and then it needs to be scheduled regularly
# echo "LOAD : loading the Biocache occurrence counts into the BIE $('date')"
# java -classpath $CLASSPATH org.ala.hbase.BiocacheDynamicLoader

echo "LOAD : loading the Limnetic data into the BIE $('date')"
java -classpath $CLASSPATH org.ala.hbase.LimneticDataLoader

echo "LOAD : loading the Specimen Holding information into the BIE $('date')"
java -classpath $CLASSPATH org.ala.hbase.SpecimenHoldingLoader Bot.20101018-1625.csv
java -classpath $CLASSPATH org.ala.hbase.SpecimenHoldingLoader Bot.20101020.csv

#NQ 2014-0210 - these 2 are now loaded into the list tool.
#echo "LOAD : loading weed information into BIE $('date')"
#java -classpath $CLASSPATH org.ala.hbase.CategoryCSVLoader dr740 1

#echo "LOAD : loading vertebrate pest information into BIE $('date')"
#java -classpath $CLASSPATH org.ala.hbase.CategoryCSVLoader dr707 1

echo "LOAD : loading CAAB family common names into BIE $('date')"
java -classpath $CLASSPATH org.ala.hbase.CommonNameCSVLoader dr809 

echo "LOAD : loading the previous guids into the current BIE $('date')"
java -classpath $CLASSPATH org.ala.hbase.PreviousGuidsLoader 


echo "LOAD : load the rankings into the new BIE $('date')"
java -classpath $CLASSPATH org.ala.util.LoadRankingColumnFamily

echo "LOAD : re-apply the rankings into the tc for BIE $('date')"
java -classpath $CLASSPATH org.ala.hbase.ImageRankUpdater

echo "Export the taxon profile information for the Biocache $('date')"
java -classpath $CLASSPATH org.ala.report.BIEProfileDump


# Export the current rankings against the old DB
#org.ala.apps.RkColumnFamilyExporter 
# Load the current rankings into the new DB by matching the guid or names.
#org.ala.util.LoadRankingColumnFamily
# update the rankings/

# This step is performed during the ala names loading
# echo "LOAD : loading the LinkIdentifier into the BIE $('date')"
# java -classpath $CLASSPATH org.ala.hbase.LinkIdentifierLoader

echo "LOAD : running Create Search Indexes from BIE for the Web Application $('date')"
java  -Xmx4g -Xms4g -classpath $CLASSPATH org.ala.lucene.CreateSearchIndex

echo "LOAD : running Create Search Indexes from External databases for the Web Application $('date')"
java  -Xmx2g -Xms2g -classpath $CLASSPATH org.ala.lucene.ExternalIndexLoader

echo "LOAD INDEX : running create WordPress Index for the Web Application $('date')"
java -Xmx1g -Xms1g -classpath $CLASSPATH org.ala.lucene.CreateWordPressIndex

echo "LOAD : optimising indexes $('date')"
java  -Xmx2g -Xms2g -classpath $CLASSPATH org.ala.lucene.OptimizeIndex

echo "LOAD : processing complete at $('date')"

finish_time=$(date +%s)

echo "LOAD : Time taken: $(( $((finish_time - start_time)) /3600 )) hours $(( $((finish_time - start_time)) /60 )) minutes."
