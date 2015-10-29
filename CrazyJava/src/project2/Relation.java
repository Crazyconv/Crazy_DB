/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2.
 * This class shall not be modified in any form. 
 */

package project2;

import java.io.BufferedReader;
import java.io.FileReader;

import java.util.ArrayList;

public class Relation {

	/**
	 * This is the name of the relation 
	 */
	protected String name; 
	
	/**
	 * This is the list of blocks contained in this relation
	 */
	private ArrayList<Block> blockLst;
	
	/**
	 * This is the writer that can be used to write blocks of tuples to this relation
	 */
	private RelationWriter rWriter;
	
	public Relation(String name) {
		this.name = name; 
		this.blockLst = new ArrayList<Block>(); 
		this.rWriter=null;
	}

	/**
	 *  @return number of tuples in this relation
	 */
	public int getNumTuples() {
		int numTuples = 0;
		for (Block b : blockLst) {
			numTuples += b.getNumTuples();
		}
		return numTuples;
	}
	
	/**
	 * @return number of blocks in this relation
	 */
	public int getNumBlocks(){
		return blockLst.size();
	}
	
		
	/**
	 * Populate this relation using the data read from a file with the given blockFactor setting 
	 * @param fileName The file containing the tuples to be stored in this relation
	 */
	public int populateRelationFromFile(String fileName){
		BufferedReader fileIn;
		Tuple tuple;
		Block block = new Block();
		int numTuples=0;
		String line = "";
		try {
			fileIn = new BufferedReader(new FileReader(fileName));
			while ((line = fileIn.readLine()) != null) {
				int key = Integer
						.parseInt(line.substring(0, line.indexOf(" ")));
				String value = line.substring(line.indexOf(" ")).trim();
				tuple = new Tuple(key, value);
				numTuples++;
				if (!block.insertTuple(tuple)) {
					blockLst.add(block);
					block = new Block();
					block.insertTuple(tuple);
				}
			}
			blockLst.add(block);
			fileIn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return numTuples;
	}
	
	/**
	 * Print this relation 
	 * @param block is a boolean flag to indicate whether to print block details 
	 * @param tuple is a boolean flag to indicate whether to print tuple details 
	 */
	public void printRelation(boolean block, boolean tuple) {
		System.out.println("Relation: " + name + "\tNumBlocks:" + getNumBlocks()+ "\tNumTuples:" + getNumTuples());
		if(block){
			for (Block b : blockLst) {
				b.print(tuple);
			}
		}
	}
	
	/**
	 * When this relation is loaded for sorting or joining, the relation shall be read through this RelationLoader
	 * @return an instance of a RelationLoader of the current relation; multiple loader instances can be created if necessary 
	 */
	public RelationLoader getRelationLoader(){
		RelationLoader rreader=new RelationLoader();
		return rreader;
	}
	
	/**
	 * The relation writer is used for writing blocks to this relation. There should be only one writer at any time of instance. 
	 * This writer writes blocks to this relation in sequential order. 
	 * @return The relation writer instance. 
	 */
	public RelationWriter getRelationWriter(){
		if(this.rWriter==null){
			this.rWriter=new RelationWriter();
		}
		return this.rWriter;
	}
	
	
	/**
	 * The RelationLoader class  
	 * @author axsun
	 *
	 */
	protected class RelationLoader{
		private int iterator=0;
		/**
		 * A private constructor to ensure a RelationLoader instance cannot be created elsewhere
		 */
		private RelationLoader(){
			this.iterator=0;
		}
		
		/**
		 * To reset the reader for a possible reading from the beginning of the relation
		 */
		public void reset(){
			iterator=0;
		}
		
		/**
		 * To test whether there are remaining blocks in the relation
		 * @return true if there blocks not read yet and false otherwise.
		 */
		public boolean hasNextBlock(){
			return iterator<blockLst.size();
		}
		
		/**
		 * Load the next n blocks from this relation and n cannot be more than size of the given memory
		 * @param n is the number of blocks to be read 
		 * @return the array containing at most n blocks read from this relation
		 */
		public Block[] loadNextBlocks(int n){
			if(n>Setting.memorySize){
				n=Setting.memorySize;
			}
			Block[] nextBlocks=new Block[n];
			for(int i=0; i<n; i++){
				if(iterator<blockLst.size()){
					nextBlocks[i]=blockLst.get(iterator);
					iterator++;
				}else{
					break;
				}
			}
			return nextBlocks;
		}

	}

	/**
	 * The RelationWriter class  
	 * @author axsun
	 *
	 */
	protected class RelationWriter{
		/**
		 * A private constructor to ensure a RelationWriter instance cannot be created elsewhere
		 */
		private RelationWriter(){	
		}
		/**
		 * Write to this relation a new block
		 * @param b The block written to the relation 
		 */
		public void writeBlock(Block b){
			blockLst.add(b);
		}
	}
}
