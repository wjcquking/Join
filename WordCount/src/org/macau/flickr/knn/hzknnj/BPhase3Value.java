package org.macau.flickr.knn.hzknnj;
import java.io.*;

import org.apache.hadoop.io.*;


public class BPhase3Value implements WritableComparable<BPhase3Value> {

	private IntWritable first;
	private FloatWritable second;

	public BPhase3Value() {
		set(new IntWritable(), new FloatWritable());
	}

	public BPhase3Value(int first, float second) {
		set(new IntWritable(first), new FloatWritable(second));
	}

	public void set(IntWritable first, FloatWritable second) {
		this.first = first;
		this.second = second;	
	}

	public IntWritable getFirst() {
		return first;
	}

	public FloatWritable getSecond() {
		return second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BPhase3Value) {
			BPhase3Value rp2v = (BPhase3Value) o;
			return first.equals(rp2v.first) && second.equals(rp2v.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first.toString() + " " + second.toString();
	}

	@Override
	public int compareTo(BPhase3Value rp2v) {
		return 1;
	}

}
