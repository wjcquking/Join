/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package org.macau.flickr.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;


public class FlickrValue implements Writable {
	
    private long id;

    //the position information
    private double lat;
    private double lon;
    private long timestamp;
    
    //tag number
    private int tag;
    
    private int tileNumber;
    
    public int getTileNumber() {
		return tileNumber;
	}

	public void setTileNumber(int tileNumber) {
		this.tileNumber = tileNumber;
	}

	//the tiles List 
    //for example 1,2,3
	//sometimes record the textual
    private String tiles;
    
    //records other information of the data
    private String others;
    
  

	public String getOthers() {
		return others;
	}

	public void setOthers(String others) {
		this.others = others;
	}

	public String getTiles() {
		return tiles;
	}

	public void setTiles(String tiles) {
		this.tiles = tiles;
	}

	public int getTag() {
		return tag;
	}

	public void setTag(int tag) {
		this.tag = tag;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public FlickrValue() {
    
    }

    public FlickrValue(long id, double lat, double lon,long timestamp) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
    }
    
    public FlickrValue(long id, double lat, double lon,long timestamp,int tag) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
        this.tag = tag;
    }
    
    public FlickrValue(long id, double lat, double lon,long timestamp,int tag,String tiles) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
        this.tag = tag;
        this.tiles = tiles;
    }
    
    public FlickrValue(long id, double lat, double lon,long timestamp,int tag,String tiles,int tileNumber) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
        this.tag = tag;
        this.tiles = tiles;
        this.tileNumber = tileNumber;
    }
    
    public FlickrValue(long id, double lat, double lon,long timestamp,int tag,String tiles,int tileNumber,String others) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
        this.tag = tag;
        this.tiles = tiles;
        this.tileNumber = tileNumber;
        this.others = others;
    }

    public FlickrValue(FlickrValue v) {
        id = v.id;
        lat = v.lat;
        lon = v.lon;
        timestamp = v.timestamp;
        tag = v.tag;
        tiles = v.tiles;
        tileNumber = v.tileNumber;
        others = v.others;
    }


    public String toFileString(){
    	return id + ";" + lat + ";" + lon + ";" + timestamp;
    }
    @Override
    public String toString() {
        return id + ";" + lat + ";" + lon + ";" + timestamp + ";" + tag + ";" + tileNumber +";" + tiles;
    }

    /**
     * the order is important 
     * when write the 
     * out.writeChars(tiles);
     * out.writeInt(tileNumber);
     * there is some mistakes in the result because they can not find the tileNumber
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeDouble(lat);
        out.writeDouble(lon);
        out.writeLong(timestamp);
        out.writeInt(tag);
        out.writeInt(tileNumber);
        out.writeUTF(tiles);
        out.writeUTF(others);
      
    }

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readLong();
		lat = in.readDouble();
		lon = in.readDouble();
		timestamp = in.readLong();
		tag = in.readInt();
		tileNumber = in.readInt();
		tiles = in.readUTF();
		others = in.readUTF();
		
	}
}
