/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2.
 * This class shall not be modified in any form. 
 */

package project2;

import java.util.ArrayList;

public class Block {
	/**
	 * List of tuples contained in this block
	 */
	protected ArrayList<Tuple> tupleLst; 

	public Block(){
		this.tupleLst=new ArrayList<Tuple>(Setting.blockFactor);
	}
	
	/**
	 * Insert a tuple t to this block
	 * @param t is a tuple to be inserted to this block
	 * @return true if tuple t is successfully inserted into this block and false if the block is full
	 */
	public boolean insertTuple(Tuple t){
		if(tupleLst.size()<Setting.blockFactor){
			tupleLst.add(t);
			return true;
		}
		return false;
	}
	
	/**
	 * @return number of tuples stored in this block
	 */
	public int getNumTuples(){
		return tupleLst.size();
	}
	
	/**
	 * Print this block 
	 * @param printTuple is a flag to indicate whether the details of the tuples are printed 
	 */
	public void print(boolean printTuple){
		System.out.println("[BlockSize: "+getNumTuples()+" Tuples]");
		if(printTuple){
			for(Tuple t:tupleLst){
				System.out.println(t);
			}
		}
	}
}
