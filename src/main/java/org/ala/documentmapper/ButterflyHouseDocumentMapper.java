/***************************************************************************
 * Copyright (C) 2009 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package org.ala.documentmapper;

import java.util.ArrayList;
import java.util.List;

import org.ala.repository.ParsedDocument;
import org.ala.repository.Predicates;
import org.ala.repository.Triple;
import org.ala.util.MimeType;
import org.ala.util.Response;
import org.ala.util.WebUtils;
import org.w3c.dom.Document;

/**
 * Document mapper for ButterflyHouse
 *
 * @author Tommy Wang (Tommy.Wang@csiro.au)
 */
public class ButterflyHouseDocumentMapper extends XMLDocumentMapper {

	/**
	 * Initialise the mapper, adding new XPath expressions
	 * for extracting content.
	 */
	public ButterflyHouseDocumentMapper() {
		
		setRecursiveValueExtraction(true);
		
		//set the content type this doc mapper handles
		this.contentType = MimeType.HTML.toString();
		
		//set an initial subject
		String subject = MappingUtils.getSubject();
		
		// subject, predicate namespace, predicate, 
		//addDCMapping("//a[@id=\"top\"]/text()", subject, Predicates.DC_TITLE);

		addDCMapping("//html/head/meta[@scheme=\"URL\" and @name=\"ALA.Guid\"]/attribute::content",subject, Predicates.DC_IDENTIFIER);
        addDCMapping("//center/font[1]/b/i[1]/text()" +
        		"|//center/h4/i[1]/text()" +
        		"|//center/b/i[1]/text()" +
        		"|//center/font[1]/i[1]/text()",subject, Predicates.DC_TITLE);
        addTripleMapping("//center/font[1]/b/i[1]/text()" +
                "|//center/h4/i[1]/text()" +
                "|//center/b/i[1]/text()" +
                "|//center/font[1]/i[1]/text()", subject, Predicates.SCIENTIFIC_NAME);
        addTripleMapping("//center//a/i/text()", subject, Predicates.FAMILY);
        addTripleMapping("//center/img[1]/@src", subject, Predicates.IMAGE_URL);
//		
////		addTripleMapping("//p[b[contains(.,\"Attached Images\")]]/following::p[1]/a/attribute::href",
//		addTripleMapping("//img[@class=\"page\"]/attribute::src",
//				subject, Predicates.IMAGE_URL);
//		
//		addTripleMapping("//a[@class=\"mappopup\"]/span/img/attribute::src",
//                subject, Predicates.DIST_MAP_IMG_URL);
	}
	
	@Override
	public List<ParsedDocument> map(String uri, byte[] content)
		throws Exception {
		
		String documentStr = new String(content);
		
//		documentStr = documentStr.replaceAll("[\\s&&[^ ]]{0,}<i>[\\s&&[^ ]]{0,}", "");
//		documentStr = documentStr.replaceAll("[\\s&&[^ ]]{0,}</i>[\\s&&[^ ]]{0,}", "");
//		documentStr = documentStr.replaceAll("[\\s&&[^ ]]{0,}<em>[\\s&&[^ ]]{0,}", "");
//		documentStr = documentStr.replaceAll("[\\s&&[^ ]]{0,}</em>[\\s&&[^ ]]{0,}", "");
		
		content = documentStr.getBytes();
		
		return super.map(uri, content);
	}

	/**
	 * @see ala.documentmapper.XMLDocumentMapper#extractProperties(org.w3c.dom.Document)
	 */
	@Override
	protected void extractProperties(List<ParsedDocument> pds, Document xmlDocument) throws Exception {
		
		// extract details of images
		
		ParsedDocument pd = pds.get(0);
		String subject = MappingUtils.getSubject();		
		String baseUrl = pd.getGuid().substring(0, pd.getGuid().lastIndexOf("/") + 1);
		
		List<Triple<String,String,String>> triples = pd.getTriples();
//		List<Triple<String,String,String>> tmpTriple = new ArrayList<Triple<String,String,String>>();
		
		triples.add(new Triple(subject, Predicates.KINGDOM.toString(), "Animalia"));
		//replace the list of triples
		
		for (Triple<String,String,String> triple: triples) {
            String predicate = triple.getPredicate().toString();
            
            if(predicate.endsWith("hasImageUrl")) {
                String imageUrl = (String) triple.getObject();
                
                imageUrl = baseUrl + imageUrl;
                
                triple.setObject(imageUrl);
            }
        }
		pd.setTriples(triples);
	}
	
}
