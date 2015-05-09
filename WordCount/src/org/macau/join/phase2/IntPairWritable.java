package org.macau.join.phase2;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * cid---the cell id 
 * token-- the prefix token
 * 
 */
public class IntPairWritable  implements WritableComparable{

	private int cid;
    private int token;
    
    public IntPairWritable(){
    	
    }
    
    public IntPairWritable(int cid,int token){
    	this.cid = cid;
    	this.token = token;
    }
    
    public IntPairWritable(IntPairWritable i){
    	this.cid = i.cid;
    	this.token = i.token;
    }
    
	public int getCid() {
		return cid;
	}

	public void setCid(int cid) {
		this.cid = cid;
	}

	public int getToken() {
		return token;
	}

	public void setToken(int token) {
		this.token = token;
	}
  
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		cid = in.readInt();
		token = in.readInt();
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(cid);
		out.writeInt(token);
	}

	public String toString(){
		return "cid is " + cid + "token " + token;
	}
	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		if (this == o) {
            return 0;
        }
        IntPairWritable p = (IntPairWritable) o;
        if (cid != p.cid) {
            return cid < p.cid ? -1 : 1;
        }
        return token < p.token ? -1 : token > p.token ? 1 : 0;
	}

}
