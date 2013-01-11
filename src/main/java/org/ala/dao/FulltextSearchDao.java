/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package org.ala.dao;

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.ala.dto.AutoCompleteDTO;

import org.ala.dto.SearchDTO;
import org.ala.dto.SearchRegionDTO;
import org.ala.dto.SearchResultsDTO;
import org.ala.dto.SearchTaxonConceptDTO;
import org.ala.util.StatusType;

/**
 * Interface for searching taxon concept profiles.
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public interface FulltextSearchDao {

	/**
	 * Retrieves a classification used nested set logic.
	 * 
	 * @param leftNSValue
	 * @return
	 * @throws Exception
	 */
    SearchResultsDTO getClassificationByLeftNS(int leftNSValue, int rightNSValue) throws Exception;	
    SearchResultsDTO getAllRankItems(String rank) throws Exception;
    /**
     * Retrieve the child taxa using the parent ID. This is a numeric ID that is internal to
     * the system
     * 
     * @param parentId
     * @return
     * @throws Exception
     */
    List<SearchTaxonConceptDTO> getChildConceptsParentId(String parentId) throws Exception;
    
	/**
	 * Retrieves a classification used nested set logic.
	 * 
	 * @param left
     * @parma right
	 * @return
	 * @throws Exception
	 */
    List<SearchTaxonConceptDTO> getChildConceptsByNS(int left, int right, Integer rankId) throws Exception;
    List<SearchTaxonConceptDTO> getChildConceptsByNS(int left, int right, Integer rankId, int maxResults) throws Exception;
    
    /**
     * Search for taxon concept with the following status type
     *
     * @param statusType
     * @param filterQuery
     * @param startIndex
     * @param pageSize
     * @param sortField
     * @param sortDirection
     * @return
     * @throws Exception
     */
    SearchResultsDTO findAllByStatus(StatusType statusType, String filterQuery, Integer startIndex,
            Integer pageSize, String sortField, String sortDirection) throws Exception;

    SearchResultsDTO findAllSpeciesByRegionAndHigherTaxon(String regionType, 
    		String regionName, String rank, String higherTaxon,
    		String filterQuery, Integer startIndex, Integer pageSize,
            String sortField, String sortDirection) throws Exception;
    
    SearchResultsDTO findAllSpeciesByRegionAndHigherTaxon(String regionType, String regionName, 
    		String rank, List<String> higherTaxa, 
    		String filterQuery, Integer startIndex, Integer pageSize,
            String sortField, String sortDirection) throws Exception;
    
    SearchResultsDTO findAllSpeciesByRegionAndHigherTaxon(String regionType, 
    		String regionName, String rank, String higherTaxon,
    		String filterQuery, Integer startIndex, Integer pageSize,
            String sortField, String sortDirection, boolean withImages) throws Exception;
    
    SearchResultsDTO findAllSpeciesByRegionAndHigherTaxon(String regionType, String regionName, 
    		String rank, List<String> higherTaxa, 
    		String filterQuery, Integer startIndex, Integer pageSize,
            String sortField, String sortDirection, boolean withImages) throws Exception;
      

    SearchResultsDTO<SearchRegionDTO> findAllRegionsByType(RegionTypes regionType) throws Exception;
    
    /**
     * Retrieves a list of species that have been recorded in one region
     * but not in another.
     * 
     * @param regionType the region type to test
     * @param regionName the region name
     * @param altRegionType the region type to compare to
     * @param altRegionName the region name to compare to
     * @param rank
     * @param higherTaxa
     * @param filterQuery
     * @param startIndex
     * @param pageSize
     * @param sortField
     * @param sortDirection
     * @return
     * @throws Exception
     */
    SearchResultsDTO findAllDifferencesInSpeciesByRegionAndHigherTaxon(
    		String regionType, String regionName, 
    		String altRegionType, String altRegionName, 
    		String rank, List<String> higherTaxa, 
    		String filterQuery, Integer startIndex, Integer pageSize,
            String sortField, String sortDirection) throws Exception;
    
    int countSpeciesByRegionAndHigherTaxon(String regionType, String regionName, 
    		 String rank, String higherTaxon) throws Exception;
    
    int countSpeciesByRegionAndHigherTaxon(String regionType,
			String regionName, String rank, List<String> higherTaxa)
			throws Exception;

    int writeSpeciesByRegionAndHigherTaxon(String regionType, String regionName, 
    		String rank, String higherTaxon, OutputStream output)
			throws Exception;
    
    int writeSpeciesByRegionAndHigherTaxon(String regionType, String regionName, 
    		String rank, List<String> higherTaxa, OutputStream output)
			throws Exception;
    
    /**
     * Writes supplied fields of the "TAXON" results to the output stream. 
     * @param q
     * @param fq
     * @param fields
     * @param output
     * @return
     */
    int writeResults(String q, String[] fq, String sort,String dir, String[] fields, OutputStream output) throws Exception;
    
    /**
     * Search for taxon concept with the following scientific name.
     *
     * @param input
     * @param limit
     * @return
     * @throws Exception
     */
    List<SearchTaxonConceptDTO> findByScientificName(String input, int limit) throws Exception;
    
    /**
     * Generic Search with name.
     *
     * @return
     * @throws Exception
     */
    SearchResultsDTO<SearchDTO> findByName(IndexedTypes indexType, String query, String[] filterQuery, Integer startIndex,
            Integer pageSize, String sortField, String sortDirection) throws Exception;
    
    /**
     * Returns the results of a list of taxon Guids.
     * @param guids
     * @return
     * @throws Exception
     */
    SearchResultsDTO<SearchDTO> findByGuids(String[] guids) throws Exception;

    /**
     * Perform a full text search across all object types.
     *
     * @param query
     * @param filterQuery
     * @param startIndex
     * @param pageSize
     * @param sortField
     * @param sortDirection
     * @return
     * @throws Exception
     */
    SearchResultsDTO<SearchDTO> doFullTextSearch(String query, String[] filterQuery, String[] facets, Integer startIndex, Integer pageSize,
    		String sortField, String sortDirection) throws Exception;
    
    /**
     * Search for taxon concept with the following scientific name with parameters for paging & sorting
     *
     * @param query
     * @param filterQuery
     * @param startIndex
     * @param pageSize
     * @param sortField
     * @param sortDirection
     * @return
     * @throws Exception
     */
    SearchResultsDTO<SearchTaxonConceptDTO> findByScientificName(String query, String[] filterQuery, Integer startIndex,
            Integer pageSize, String sortField, String sortDirection) throws Exception;

    /**
     * For every dataset (infoSource) get a count of the number of taxon concepts which contain indexed
     * information from that dataset.
     *
     * @return
     * @throws Exception
     */
    Map<String, Long> getAllDatasetCounts() throws Exception;

    /**
     * Provides a list of auto complete DTO's that for the value text. Preference is
     * given for matches at the start of the term.
     * @param value
     * @param indexType
     * @param gsOnly
     * @param maxTerms
     * @return
     * @throws Exception
     */
    List<AutoCompleteDTO> getAutoCompleteList(String value,String[] filterQuery,IndexedTypes indexType, boolean gsOnly, int maxTerms) throws Exception;
    
    /**
     * ranking search functions
     */
    public Collection getRankingFacetByUserIdAndGuid(String userId, String guid) throws Exception;
    public Collection getUserIdFacetByGuid(String guid) throws Exception;

    SearchResultsDTO<SearchDTO> findByUid(IndexedTypes indexType, String query,
            String[] filterQuery, Integer startIndex, Integer pageSize,
            String sortField, String sortDirection) throws Exception;
    
	public SearchResultsDTO<SearchDTO> doExactTextSearch(String query, String[] filterQuery, Integer startIndex, Integer pageSize, String sortField, String sortDirection) throws Exception;

}
