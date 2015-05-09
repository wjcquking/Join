package org.macau.local.util;

public class FlickrData {

    private long id;

    //the position information
    private int locationID;
    private double lat;
    private double lon;
    private long timestamp;
    
    private String textual;

    private int[] tokens;
    
    public int[] getTokens() {
		return tokens;
	}



	public void setTokens(int[] tokens) {
		this.tokens = tokens;
	}



	public int getLocationID() {
		return locationID;
	}



	public void setLocationID(int locationID) {
		this.locationID = locationID;
	}

	public String getTextual() {
		return textual;
	}

	public void setTextual(String textual) {
		this.textual = textual;
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
	
	
	public String toString(){
		return id + ":" + locationID + ":" + lat + ":" + lon + ":" + timestamp + ":" + textual;
	}
	
	@Override 
	public int hashCode(){
	    return FlickrData.class.toString().hashCode();
	 }
	@Override 
	public boolean equals(Object object){
	    if(object==null) return this==null;
	    return object.toString().equals(toString());
	  }
	public FlickrData() {
    
    }
	
	public FlickrData(FlickrData fd){
		
		this.id = fd.id;
		this.locationID = fd.locationID;
		this.lat = fd.lat;
		this.lon = fd.lon;
		this.timestamp = fd.timestamp;
		this.textual = fd.textual;
		
	}
	
	public FlickrData(long id, int locationID, double lat, double lon, long timestamp,String textual){
		this.id = id;
		this.locationID = locationID;
		this.lat = lat;
		this.lon = lon;
		this.timestamp = timestamp;
		this.textual = textual;
	}
	
	public FlickrData(long id, int locationID, double lat, double lon, long timestamp,String textual,int[] tokens){
		this.id = id;
		this.locationID = locationID;
		this.lat = lat;
		this.lon = lon;
		this.timestamp = timestamp;
		this.textual = textual;
		this.tokens = tokens;
	}
	

    public FlickrData(long id, double lat, double lon,long timestamp) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
    }
}
