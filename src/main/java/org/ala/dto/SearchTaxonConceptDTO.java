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
package org.ala.dto;

import java.io.InputStream;
import java.util.Properties;

/**
 * A DTO used for returning search results.
 *
 * @author Dave Martin (David.Martin@csiro.au)
 */
public class SearchTaxonConceptDTO extends SearchDTO implements Comparable<SearchTaxonConceptDTO>{

    protected static String bieRepoUrl = "http://bie.ala.org.au/repo/";
    protected static String bieRepoDir = "/data/bie/";

    static {
        //check the properties file for an override
        try {
            Properties p = new Properties();
            InputStream inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("bie.properties");
            p.load(inStream);
            if(p.getProperty("bieRepo") != null)
                bieRepoUrl = p.getProperty("bieRepo");
            if(p.getProperty("bieRepoDir") != null)
                bieRepoDir = p.getProperty("bieRepoDir");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

	protected String parentId;
	protected String parentGuid;
	protected String commonName;
	protected String nameComplete;
	protected String commonNameSingle;
	protected String acceptedConceptGuid;
	protected String acceptedConceptName;
	protected boolean hasChildren;
    protected String rank;
    protected int rankId;
    protected String rawRank;
    protected String pestStatus;
    protected String conservationStatus;
    protected String conservationStatusAUS;
    protected String conservationStatusACT;
	protected String conservationStatusNSW;
    protected String conservationStatusNT;
    protected String conservationStatusQLD;
    protected String conservationStatusSA;
    protected String conservationStatusTAS;
    protected String conservationStatusVIC;
    protected String conservationStatusWA;
    protected String isAustralian;
    protected String highlight;
    protected String image;
    protected String thumbnail;
    protected Integer left;
    protected Integer right;
    protected String kingdom;
    protected String author;
    protected String linkIdentifier;
    protected Integer occCount;
    protected String imageSource;
    protected Integer imageCount;
    protected Boolean isExcluded; //whether or not the taxon concept has been marked as excluded in Australia

    //image properties
    String imageUrl;
    String largeImageUrl;
    String smallImageUrl;
    String thumbnailUrl;
    String imageMetadataUrl;

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public void setLargeImageUrl(String largeImageUrl) {
        this.largeImageUrl = largeImageUrl;
    }

    public void setSmallImageUrl(String smallImageUrl) {
        this.smallImageUrl = smallImageUrl;
    }

    public void setThumbnailUrl(String thumbnailUrl) {
        this.thumbnailUrl = thumbnailUrl;
    }

    public void setImageMetadataUrl(String imageMetadataUrl) {
        this.imageMetadataUrl = imageMetadataUrl;
    }

    public String getImageUrl(){
        if(imageUrl == null){
            if(image != null && image.startsWith(bieRepoDir)){
                imageUrl= image.replace(bieRepoDir, bieRepoUrl);
            }
        }
        return imageUrl;
    }

    private String getImageUrl(String image, String imageFormatName){
        if(image !=null){
            if(image.startsWith(bieRepoDir)){
                image = image.replace(bieRepoDir, bieRepoUrl);
            }
            return image.replace("/raw.", imageFormatName);
        } else {
            return null;
        }
    }

    public String getLargeImageUrl(){
        this.largeImageUrl = getImageUrl(image, "/largeRaw.");
        return this.largeImageUrl;
    }

    public String getSmallImageUrl(){
        this.smallImageUrl = getImageUrl(image, "/smallRaw.");
        return smallImageUrl;
    }

    public String getThumbnailUrl(){
        this.thumbnailUrl = getImageUrl(image, "/thumbnail.");
        return thumbnailUrl;
    }

    public String getImageMetadataUrl(){
        if(imageMetadataUrl ==null){
            if(image != null && image.startsWith(bieRepoDir)){
                image =  image.replace(bieRepoDir, bieRepoUrl);
            }
            if(image !=null){
                this.imageMetadataUrl = image.substring(0, image.lastIndexOf('/') +1) + "dc";
            }
        }
        return this.imageMetadataUrl;
    }

	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(SearchTaxonConceptDTO o) {
		if(o.getName()!=null && this.name!=null){
			return this.name.compareTo(o.getName());
		}
		return 0;
	}
	
	/**
	 * @return the parentId
	 */
	public String getParentId() {
		return parentId;
	}
	/**
	 * @param parentId the parentId to set
	 */
	public void setParentId(String parentId) {
		this.parentId = parentId;
	}
	/**
	 * @return the commonName
	 */
	public String getCommonName() {
		return commonName;
	}
	/**
	 * @param commonName the commonName to set
	 */
	public void setCommonName(String commonName) {
		this.commonName = commonName;
	}
	/**
	 * @return the acceptedConceptGuid
	 */
	public String getAcceptedConceptGuid() {
		return acceptedConceptGuid;
	}
	/**
	 * @param acceptedConceptGuid the acceptedConceptGuid to set
	 */
	public void setAcceptedConceptGuid(String acceptedConceptGuid) {
		this.acceptedConceptGuid = acceptedConceptGuid;
	}
	/**
	 * @return the acceptedConceptName
	 */
	public String getAcceptedConceptName() {
		return acceptedConceptName;
	}
	/**
	 * @param acceptedConceptName the acceptedConceptName to set
	 */
	public void setAcceptedConceptName(String acceptedConceptName) {
		this.acceptedConceptName = acceptedConceptName;
	}
	/**
	 * @return the parentGuid
	 */
	public String getParentGuid() {
		return parentGuid;
	}
	/**
	 * @param parentGuid the parentGuid to set
	 */
	public void setParentGuid(String parentGuid) {
		this.parentGuid = parentGuid;
	}
	/**
	 * @return the hasChildren
	 */
	public boolean isHasChildren() {
		return hasChildren;
	}
	/**
	 * @param hasChildren the hasChildren to set
	 */
	public void setHasChildren(boolean hasChildren) {
		this.hasChildren = hasChildren;
	}
	/**
	 * @return the rank
	 */
	public String getRank() {
		return rank;
	}
	/**
	 * @param rank the rank to set
	 */
	public void setRank(String rank) {
		this.rank = rank;
	}
	/**
	 * @return the rankId
	 */
	public int getRankId() {
		return rankId;
	}
	/**
	 * @param rankId the rankId to set
	 */
	public void setRankId(int rankId) {
		this.rankId = rankId;
	}
	/**
	 * @return the pestStatus
	 */
	public String getPestStatus() {
		return pestStatus;
	}
	/**
	 * @param pestStatus the pestStatus to set
	 */
	public void setPestStatus(String pestStatus) {
		this.pestStatus = pestStatus;
	}
	/**
	 * @return the conservationStatus
	 */
	public String getConservationStatus() {
		return conservationStatus;
	}
    /**
	 * @return the conservationStatusAUS
	 */
	public String getConservationStatusAUS() {
		return conservationStatusAUS;
	}

	/**
	 * @param conservationStatusAUS the conservationStatusAUS to set
	 */
	public void setConservationStatusAUS(String conservationStatusAUS) {
		this.conservationStatusAUS = conservationStatusAUS;
	}

	/**
	 * @return the conservationStatusACT
	 */
	public String getConservationStatusACT() {
		return conservationStatusACT;
	}

	/**
	 * @param conservationStatusACT the conservationStatusACT to set
	 */
	public void setConservationStatusACT(String conservationStatusACT) {
		this.conservationStatusACT = conservationStatusACT;
	}

	/**
	 * @return the conservationStatusNSW
	 */
	public String getConservationStatusNSW() {
		return conservationStatusNSW;
	}

	/**
	 * @param conservationStatusNSW the conservationStatusNSW to set
	 */
	public void setConservationStatusNSW(String conservationStatusNSW) {
		this.conservationStatusNSW = conservationStatusNSW;
	}

	/**
	 * @return the conservationStatusNT
	 */
	public String getConservationStatusNT() {
		return conservationStatusNT;
	}

	/**
	 * @param conservationStatusNT the conservationStatusNT to set
	 */
	public void setConservationStatusNT(String conservationStatusNT) {
		this.conservationStatusNT = conservationStatusNT;
	}

	/**
	 * @return the conservationStatusQLD
	 */
	public String getConservationStatusQLD() {
		return conservationStatusQLD;
	}

	/**
	 * @param conservationStatusQLD the conservationStatusQLD to set
	 */
	public void setConservationStatusQLD(String conservationStatusQLD) {
		this.conservationStatusQLD = conservationStatusQLD;
	}

	/**
	 * @return the conservationStatusSA
	 */
	public String getConservationStatusSA() {
		return conservationStatusSA;
	}

	/**
	 * @param conservationStatusSA the conservationStatusSA to set
	 */
	public void setConservationStatusSA(String conservationStatusSA) {
		this.conservationStatusSA = conservationStatusSA;
	}

	/**
	 * @return the conservationStatusTAS
	 */
	public String getConservationStatusTAS() {
		return conservationStatusTAS;
	}

	/**
	 * @param conservationStatusTAS the conservationStatusTAS to set
	 */
	public void setConservationStatusTAS(String conservationStatusTAS) {
		this.conservationStatusTAS = conservationStatusTAS;
	}

	/**
	 * @return the conservationStatusVIC
	 */
	public String getConservationStatusVIC() {
		return conservationStatusVIC;
	}

	/**
	 * @param conservationStatusVIC the conservationStatusVIC to set
	 */
	public void setConservationStatusVIC(String conservationStatusVIC) {
		this.conservationStatusVIC = conservationStatusVIC;
	}

	/**
	 * @return the conservationStatusWA
	 */
	public String getConservationStatusWA() {
		return conservationStatusWA;
	}

	/**
	 * @param conservationStatusWA the conservationStatusWA to set
	 */
	public void setConservationStatusWA(String conservationStatusWA) {
		this.conservationStatusWA = conservationStatusWA;
	}
	/**
	 * @param conservationStatus the conservationStatus to set
	 */
	public void setConservationStatus(String conservationStatus) {
		this.conservationStatus = conservationStatus;
	}
	/**
	 * @return the highlight
	 */
	public String getHighlight() {
		return highlight;
	}
	/**
	 * @param highlight the highlight to set
	 */
	public void setHighlight(String highlight) {
		this.highlight = highlight;
	}
	/**
	 * @return the image
	 */
	public String getImage() {
		return image;
	}
	/**
	 * @param image the image to set
	 */
	public void setImage(String image) {
		this.image = image;
	}
	/**
	 * @return the thumbnail
	 */
	public String getThumbnail() {
		return thumbnail;
	}
	/**
	 * @param thumbnail the thumbnail to set
	 */
	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}
	/**
	 * @return the left
	 */
	public Integer getLeft() {
		return left;
	}
	/**
	 * @param left the left to set
	 */
	public void setLeft(Integer left) {
		this.left = left;
	}
	/**
	 * @return the right
	 */
	public Integer getRight() {
		return right;
	}
	/**
	 * @param right the right to set
	 */
	public void setRight(Integer right) {
		this.right = right;
	}
	/**
	 * @return the commonNameSingle
	 */
	public String getCommonNameSingle() {
		return commonNameSingle;
	}
	/**
	 * @param commonNameSingle the commonNameSingle to set
	 */
	public void setCommonNameSingle(String commonNameSingle) {
		this.commonNameSingle = commonNameSingle;
	}
    /**
	 * @return the kingdom
	 */
    public String getKingdom() {
        return kingdom;
    }
    /**
	 * @param kingdom the kingdom to set
	 */
    public void setKingdom(String kingdom) {
        this.kingdom = kingdom;
    }
    /**
	 * @return the author
	 */
    public String getAuthor() {
        return author;
    }
    /**
	 * @param author the author to set
	 */
    public void setAuthor(String author) {
        this.author = author;
    }

	/**
	 * @return the nameComplete
	 */
	public String getNameComplete() {
		return nameComplete;
	}

	/**
	 * @param nameComplete the nameComplete to set
	 */
	public void setNameComplete(String nameComplete) {
		this.nameComplete = nameComplete;
	}

    /**
	 * @return the isAustralian
	 */
    public String getIsAustralian() {
        return isAustralian;
    }

    /**
	 * @param australian the isAustralian to set
	 */
    public void setIsAustralian(String australian) {
        this.isAustralian = australian;
    }

	/**
	 * @return the linkIdentifier
	 */
	public String getLinkIdentifier() {
		return linkIdentifier;
	}

	/**
	 * @param linkIdentifier the linkIdentifier to set
	 */
	public void setLinkIdentifier(String linkIdentifier) {
		this.linkIdentifier = linkIdentifier;
	}
	
	public Integer getOccCount() {
		return occCount;
	}

	public void setOccCount(Integer occCount) {
		this.occCount = occCount;
	}

    /**
     * @return the rawRank
     */
    public String getRawRank() {
        return rawRank;
    }

    /**
     * @param rawRank the rawRank to set
     */
    public void setRawRank(String rawRank) {
        this.rawRank = rawRank;
    }	

    public String getImageSource() {
        return imageSource;
    }

    public void setImageSource(String imageSource) {
        this.imageSource = imageSource;
    }

    public Integer getImageCount() {
        return imageCount;
    }

    public void setImageCount(Integer imageCount) {
        this.imageCount = imageCount;
    }

    /**
     * @return the isExcluded
     */
    public Boolean getIsExcluded() {
        return isExcluded;
    }

    /**
     * @param isExcluded the isExcluded to set
     */
    public void setIsExcluded(Boolean isExcluded) {
        this.isExcluded = isExcluded;
    }
}