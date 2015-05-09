package org.macau.local.util;

public class WeightData {
	private double key;
	private int count;
	
	
	public WeightData(){
		
	}
	
	public WeightData(double key, int count){
		this.key = key;
		this.count = count;
	}
	public double getKey() {
		return key;
	}
	public void setKey(double key) {
		this.key = key;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
}
