package org.macau.flickr.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlickrPartitionValue implements Writable{

	private int pid;
	private int dataset;
	private double distance;
	private long id;
	
    private double lat;
    private double lon;
    
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
	public int getPid() {
		return pid;
	}
	public void setPid(int pid) {
		this.pid = pid;
	}
	public int getDataset() {
		return dataset;
	}
	public void setDataset(int dataset) {
		this.dataset = dataset;
	}
	public double getDistance() {
		return distance;
	}
	public void setDistance(double distance) {
		this.distance = distance;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	
	public FlickrPartitionValue(){
		
	}
	public FlickrPartitionValue(FlickrPartitionValue fpv){
		pid = fpv.pid;
		dataset = fpv.dataset;
		distance = fpv.distance;
		id = fpv.id;
		lat = fpv.lat;
		lon = fpv.lon;
		
	}
	public FlickrPartitionValue(int pid,int dataset,double distance,long id){
		this.pid = pid;
		this.dataset = dataset;
		this.distance = distance;
		this.id = id;
	}
	
	public String toString(){
//		return lat + "        ";
		return pid + ";" + dataset + ";" + distance+ ";"+ id;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		pid = in.readInt();
		dataset = in.readInt();
		distance = in.readDouble();
		id = in.readLong();
		lat = in.readDouble();
		lon = in.readDouble();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeInt(pid);
		out.writeInt(dataset);
		out.writeDouble(distance);
		out.writeLong(id);
		out.writeDouble(lat);
		out.writeDouble(lon);
		
	}
}
