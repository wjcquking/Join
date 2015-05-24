package org.macau.local.partition;

public class Point {

	private long x;
	private long y;
	
	private int[] boundPoint = new int[MinMaxComputationPartition.k];
	private long[] partitionValue = new long[MinMaxComputationPartition.k];
	
	public Point(){
		x = 0;
		y = 0;
	}
	
	
	public int[] getBoundPoint() {
		return boundPoint;
	}


	public void setBoundPoint(int[] boundPoint) {
		this.boundPoint = boundPoint;
	}


	public long[] getPartitionValue() {
		return partitionValue;
	}


	public void setPartitionValue(long[] partitionValue) {
		this.partitionValue = partitionValue;
	}


	public Point(long x, long y){
		this.x = x;
		this.y = y;
	}
	
	
	/***************************
	 * 
	 * @param point
	 * 
	 * NOTE: Remember use the clone() to deep copy, otherwise, it is shallow copy
	 * 
	 * If we do not use the function clone(), then If we change one value, the copy one is 
	 * also change 
	 **************************/
	public Point(Point point){
		
		this.x = point.getX();
		this.y = point.getY();
		this.boundPoint = point.boundPoint.clone();
		this.partitionValue = point.partitionValue.clone();
		
	}
	
	public long getX() {
		return x;
	}
	public void setX(long x) {
		this.x = x;
	}
	public long getY() {
		return y;
	}
	public void setY(long y) {
		this.y = y;
	}
	
	public String toString(){
		return this.x + "  " + this.y;
	}
}
