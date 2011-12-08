/***************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia
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
package org.ala.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * Only reads a tab file with all fields delimited by double quotes.
 * 
 * Written because CSVReader throws OutofMemory - not sure why.
 * 
 * @author Dave Martin
 */
public class TabReader {

	Pattern p = null;
	BufferedReader br;
	boolean hasQuotes = true;
	
	/**
	 * Initialise suppling the absolute file path.
	 * 
	 * @param filePath the absolute file path
	 * @throws Exception
	 */
	public TabReader(String filePath) throws Exception{
    	FileReader fr = new FileReader(new File(filePath));
    	this.br = new BufferedReader(fr);
    	p = Pattern.compile("\t");
	}

	public TabReader(String filePath, boolean hasQuotes) throws Exception{
    	this(filePath);
    	this.hasQuotes = hasQuotes;
    	p = Pattern.compile("\t");
	}

	public TabReader(String filePath, boolean hasQuotes, char delimiter) throws Exception{
    	this(filePath);
    	this.hasQuotes = hasQuotes;
    	p = Pattern.compile(",");
	}
	
	/**
	 * Read next line, and tokenise.
	 * 
	 * @return null if eof
	 * @throws Exception
	 */
	public String[] readNext() throws Exception{
		String line = br.readLine();
		if(line==null)
			return null;
		String[] fields = p.split(line,-1);
		for(int i=0;i<fields.length;i++){
			if(hasQuotes && fields[i].length()>=2){
				//remove quotes				
				fields[i] = fields[i].substring(1, fields[i].length()-1);
			}
			fields[i] = StringUtils.trimToNull(fields[i]);
		}
		return fields;
	}

	public void close() throws Exception {
		this.br.close();
	}
}
