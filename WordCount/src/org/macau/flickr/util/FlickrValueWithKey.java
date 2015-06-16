package org.macau.flickr.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlickrValueWithKey extends FlickrValue{

	private long key;


	
	public long getKey() {
		return key;
	}

	public void setKey(long key) {
		this.key = key;
	}

	public FlickrValueWithKey(){
		
	}
	
	public FlickrValueWithKey(FlickrValueWithKey v) {
        this.setId(v.getId());
        this.setLat(v.getLat());
        this.setLon(v.getLon());
        this.setTimestamp(v.getTimestamp());
        this.setTag(v.getTag());
        this.setTileNumber(v.getTileNumber());
        this.setTiles(v.getTiles());
        this.setOthers(v.getOthers());
        this.setKey(v.getKey());
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
        out.writeLong(this.getKey());
      
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
		this.setKey(in.readLong());
		
	}
}
