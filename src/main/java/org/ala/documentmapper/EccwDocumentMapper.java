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
package org.ala.documentmapper;

import java.util.Iterator;
import java.util.List;

import org.ala.repository.ParsedDocument;
import org.ala.repository.Predicates;
import org.ala.repository.Triple;
import org.ala.util.MimeType;
import org.w3c.dom.Document;

/**
 * Environment, Climate Change & Water NSW document mapper
 *
 * @author "Tommy Wang <Tommy.Wang@csiro.au>"
 */
public class EccwDocumentMapper extends XMLDocumentMapper {
    /**
	 * Initialise the mapper, adding new XPath expressions
	 * for extracting content.
	 */
	public EccwDocumentMapper() {
        //override the default content type
		this.contentType = MimeType.HTML.toString();
		
		setRecursiveValueExtraction(true);

		String subject = MappingUtils.getSubject();
		
        addDCMapping("//html/head/meta[@scheme=\"URL\" and @name=\"ALA.Guid\"]/attribute::content", 
        		subject, Predicates.DC_IDENTIFIER);

        addDCMapping("//h1/text()", subject, Predicates.DC_TITLE);
        
        addTripleMapping("//h1/text()",
        		 subject, Predicates.SCIENTIFIC_NAME);
        
        addTripleMapping("//h2[contains(.,\"look like\")]/following-sibling::p[1]",
        		subject, Predicates.MORPHOLOGICAL_TEXT);
        
        addTripleMapping("//h2[contains(.,\"live\")]/following-sibling::p[1]",
          		 subject, Predicates.DISTRIBUTION_TEXT);
        
        addTripleMapping("//h2[contains(.,\"Breeding\")]/following-sibling::p[1]",
         		 subject, Predicates.REPRODUCTION_TEXT);
        
        addTripleMapping("//h2[contains(.,\"What do they eat\")]/following-sibling::p[1]",
        		 subject, Predicates.DIET_TEXT);
        
        addTripleMapping("//h2[contains(.,\"Growing up\")]/following-sibling::p[1]" +
        		"|//h2[contains(.,\"Family life\")]/following-sibling::p[1]",
        		subject, Predicates.DESCRIPTIVE_TEXT);
        
        addTripleMapping("//h2[contains(.,\"Threats\")]/following-sibling::p[1]",
        		subject, Predicates.THREATS_TEXT);
        
        addTripleMapping("//div[@id=\"relatedLinks\"]//img/attribute::src",
          		 subject, Predicates.IMAGE_URL);

        
    }
	
	@Override
	public List<ParsedDocument> map(String uri, byte[] content)
	throws Exception {

		String documentStr = new String(content);

		documentStr = documentStr.replaceAll("<br/>", "");
		documentStr = documentStr.replaceAll("<ul[a-zA-Z=\\\"\\s0-9\\-]{0,}>", "<p>");
		documentStr = documentStr.replaceAll("</ul>", "</p>");
		documentStr = documentStr.replaceAll("</li>", ";</li>");
		documentStr = documentStr.replaceAll("[\\s]{2,}", " ");
		documentStr = documentStr.replaceAll("</p>[\\s]{0,}<p>", " ");
		documentStr = documentStr.replaceAll("> <", "><");
//		System.out.println(documentStr);

		content = documentStr.getBytes();

		return super.map(uri, content);
	}

    /**
	 * @see ala.documentmapper.XMLDocumentMapper#extractProperties(org.w3c.dom.Document)
	 */
	@Override
	protected void extractProperties(List<ParsedDocument> pds, Document xmlDocument) throws Exception {
        ParsedDocument pd = pds.get(0);
		List<Triple<String,String,String>> triples = pd.getTriples();
        String source = "http://www.environment.nsw.gov.au";

        pd.getDublinCore().put(Predicates.DC_LICENSE.toString(), "http://www.environment.nsw.gov.au/help/copyright.htm");
        
		for (Iterator<Triple<String,String,String>> iter = triples.iterator(); iter.hasNext(); ) {
			Triple<String,String,String> triple = iter.next();
            String predicate = triple.getPredicate().toString();

			if (predicate.equals(Predicates.IMAGE_URL.toString())) {
                // Fix relative URLs
                String imageUrl = source + (String) triple.getObject();
                triple.setObject(imageUrl);
                ParsedDocument imageDoc = MappingUtils.retrieveImageDocument(pd, imageUrl);
                if(imageDoc!=null){
                	pds.add(imageDoc);
                }
            }
        }

        // Add hard-coded Kingdom Animalia as we know this is a Zoological resource
        String subject = MappingUtils.getSubject();
        Triple<String,String,String> newTriple = new Triple<String,String,String>(subject, Predicates.KINGDOM.toString(), "Animalia");
        triples.add(newTriple);
    }
}
