/**
 * Copyright (c) CSIRO Australia, 2009
 * All rights reserved.
 *
 * Original Author: hwa002
 * Last Modified By: $LastChangedBy: hwa002 $
 * Last Modified Info: $Id: AbrsFloraOfOzOnlineDocumentMapper.java 2114 2009-12-16 23:09:34Z hwa002 $
 */

package org.ala.documentmapper;

import java.util.ArrayList;
import java.util.List;

import org.ala.repository.ParsedDocument;
import org.ala.repository.Predicates;
import org.ala.repository.Triple;
import org.ala.util.MimeType;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;

/**
 * Document Mapper for A Guide to the Marine Molluscs of Tasmania 
 *
 * @author Tommy Wang (tommy.wang@csiro.au)
 */
public class MotHtmlDocumentMapper extends XMLDocumentMapper {

	public MotHtmlDocumentMapper() {

		setRecursiveValueExtraction(true);

		this.contentType = MimeType.HTML.toString();

		String subject = MappingUtils.getSubject();

		// Extracts the unique ID from supplied page.
		// Unique ID is assumed to be embedded inside a <meta> element
		// generated by Protocol Handler.
		// Location is a HTML <meta> tag in /html/head
		addDCMapping("//html/head/meta[@scheme=\"URL\" and @name=\"ALA.Guid\"]/attribute::content", subject, Predicates.DC_IDENTIFIER);

		// Extracts the entire taxon name string
		// According to TDWG's TaxonName standard
		// http://rs.tdwg.org/ontology/voc/TaxonName#nameComplete
		// "Every TaxonName should have a DublinCore:title property that contains
		// the complete name string including authors and year (where appropriate)."

		addDCMapping("//html/head/title/text()", subject, Predicates.DC_TITLE);

		addTripleMapping("//html/head/title/text()", subject, Predicates.SCIENTIFIC_NAME);

		addTripleMapping("//font[b[contains(., 'Habitat and distribution')]]", subject, Predicates.HABITAT_TEXT);

		addTripleMapping("//html/body/descendant::a[6]/font[1]/text()", subject, Predicates.FAMILY);

		addTripleMapping("//html/body/descendant::a[7]/i/font[1]/text()", subject, Predicates.GENUS);

		addTripleMapping("//html/body/descendant::*[contains(., 'Synonym')]/i/text()", subject, Predicates.SYNONYM);

		addTripleMapping("//html/body/descendant::font[contains(text(), 'range')]/text()", subject, Predicates.DISTRIBUTION_TEXT);

		addTripleMapping("//img/attribute::src", subject, Predicates.DIST_MAP_IMG_URL);

	} // End of default constructor.


	@Override
	protected void extractProperties(List<ParsedDocument> pds, Document xmlDocument) throws Exception {

		ParsedDocument pd = pds.get(0);
		List<Triple<String,String,String>> triples = pd.getTriples();

		List<Triple<String,String,String>> toRemove = new ArrayList<Triple<String,String,String>>();

		String subject = MappingUtils.getSubject();
		String source = "http://www.molluscsoftasmania.net";

		for (Triple<String,String,String> triple: triples) {
			String predicate = triple.getPredicate().toString();
			if(predicate.endsWith("hasDistributionMapImageUrl")) {
				String imageUrl = (String) triple.getObject();
				imageUrl = imageUrl.replaceAll("\\.\\.", "");
				imageUrl = imageUrl.replaceAll(" ", "%20");
				imageUrl = source + imageUrl;

				if (imageUrl.contains("Species%20maps")) {

					ParsedDocument imageDoc = MappingUtils.retrieveImageDocument(pd, imageUrl);
					if(imageDoc!=null){
						List<Triple<String,String,String>> imageDocTriples = imageDoc.getTriples();
						imageDocTriples.add(new Triple(subject,Predicates.DIST_MAP_IMG_URL.toString(), imageDoc.getGuid()));
						imageDoc.setTriples(imageDocTriples);
						pds.add(imageDoc);
					}
				}
				//remove the triple from the triples
				toRemove.add(triple);
			}
		}

		triples.removeAll(toRemove);

		triples.add(new Triple(subject, Predicates.KINGDOM.toString(), "Animalia"));

		pd.setTriples(triples);

	} // End of `postProcessProperties` method.
}
