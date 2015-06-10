package org.macau.flickr.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlickrValueWithMultiFeature extends FlickrValue{

	private String candidateTags;

	public String getCandidateTags() {
		return candidateTags;
	}
	
	private String tileTag;
	private long uploadtime;
	private long serverId;
	private String device;
	private String description;
	
	
	public String getTileTag() {
		return tileTag;
	}

	public void setTileTag(String tileTag) {
		this.tileTag = tileTag;
	}
	
	public long getUploadtime() {
		return uploadtime;
	}

	public void setUploadtime(long uploadtime) {
		this.uploadtime = uploadtime;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	public String getDevice() {
		return device;
	}

	public void setDevice(String device) {
		this.device = device;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	

	public void setCandidateTags(String candidateTags) {
		this.candidateTags = candidateTags;
	}
	
	public FlickrValueWithMultiFeature(){
		
	}
	
	public FlickrValueWithMultiFeature(FlickrValueWithMultiFeature v) {
        this.setId(v.getId());
        this.setLat(v.getLat());
        this.setLon(v.getLon());
        this.setTimestamp(v.getTimestamp());
        this.setTag(v.getTag());
        this.setTileNumber(v.getTileNumber());
        this.setTiles(v.getTiles());
        this.setOthers(v.getOthers());
        this.setCandidateTags(v.getCandidateTags());

        this.setTileTag(v.getTileTag());
    	this.setUploadtime(uploadtime);
    	this.setServerId(serverId);
    	this.setDevice(device);
    	this.setDescription(description);
    }
	/**
     * the order is important 
     * when write the 
     * out.writeChars(tiles);
     * out.writeInt(tileNumber);
     * there is some mistakes in the result because they can not find the tileNumber
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.getId());
        out.writeDouble(this.getLat());
        out.writeDouble(this.getLon());
        out.writeLong(this.getTimestamp());
        out.writeInt(this.getTag());
        out.writeInt(this.getTileNumber());
        out.writeUTF(this.getTiles());
        out.writeUTF(this.getOthers());
        out.writeUTF(this.getCandidateTags());

        out.writeUTF(this.getTileTag());
    	out.writeLong(this.getUploadtime());
    	out.writeLong(this.getServerId());
    	out.writeUTF(this.getDevice());
    	out.writeUTF(this.getDescription());
      
    }

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.setId(in.readLong());
		this.setLat(in.readDouble());
		this.setLon(in.readDouble());
		this.setTimestamp(in.readLong());
		this.setTag(in.readInt());
		this.setTileNumber(in.readInt());
		this.setTiles(in.readUTF());
		this.setOthers(in.readUTF());
		this.setCandidateTags(in.readUTF());
		
		this.setTileTag(in.readUTF());
		this.setUploadtime(in.readLong());
		this.setServerId(in.readLong());
		this.setDevice(in.readUTF());
		this.setDescription(in.readUTF());
		
	}
}
