package org.macau.local.sample.partition;

/**
 * 
 * @author mb25428
 * This part is to get the optimal
 */
public class PartitionAccount {
	private int rAccount;
	private int sAccount;
	private double product;
	
	
	
	public int getrAccount() {
		return rAccount;
	}
	public void setrAccount(int rAccount) {
		this.rAccount = rAccount;
	}
	public int getsAccount() {
		return sAccount;
	}
	public void setsAccount(int sAccount) {
		this.sAccount = sAccount;
	}
	public double getProduct() {
		return product;
	}
	public void setProduct(double product) {
		this.product = product;
	}
	
	PartitionAccount(int rAccount, int sAccount, double product){
		this.rAccount = rAccount;
		this.sAccount = sAccount;
		this.product = product;
	}
	
}
