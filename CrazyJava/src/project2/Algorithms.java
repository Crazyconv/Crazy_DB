/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

package project2;

import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class Algorithms {
	
	/**
	 * Sort the relation using Setting.memorySize buffers of memory 
	 * @param rel is the relation to be sorted. 
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int mergeSortRelation(Relation rel){
		int numIO=0;

		// Check Memory enough.

		RelationLoader rLoader = rel.getRelationLoader();
		// Phase 1: create sorted sublist
		ArrayList<Relation> sortedSubList = new ArrayList<Relation>(Setting.memorySize - 1);

		while (rLoader.hasNextBlock()) {
			Block[] blocks = rLoader.loadNextBlocks(Setting.memorySize);

			ArrayList<Tuple> tuples = new ArrayList<Tuple>(Setting.blockFactor * Setting.memorySize);
			for (Block b : blocks) {
				if (b != null) {
					for (Tuple t : b.tupleLst)
						tuples.add(t);
					numIO++;
				}
			}
			Comparator<Tuple> newcomparator = new Comparator<Tuple>() {
				public int compare(Tuple t1, Tuple t2) {
					return t1.key - t2.key;
				}
			};
			Collections.sort(tuples, newcomparator);

			// Convert list of tuples to relations
			Block sortBlock = new Block();
			Relation sortedSubRelation = new Relation("sorted");
			RelationWriter rWriter = sortedSubRelation.getRelationWriter();
			for (Tuple t : tuples) {
				if (!sortBlock.insertTuple(t)) {
					rWriter.writeBlock(sortBlock);
					sortBlock = new Block();
					sortBlock.insertTuple(t);
					numIO++;
				}
			}
			rWriter.writeBlock(sortBlock);
			sortedSubList.add(sortedSubRelation);
			numIO++;
		}

		// Phase 2: merge
		int numsortedR = sortedSubList.size();
		Block outputBuffer = new Block();
		Block[] inputBuffer = new Block[numsortedR];
		RelationLoader[] inputLoader = new RelationLoader[numsortedR];
		Relation sortRelation = new Relation("sorted_relation");
		RelationWriter rSortWriter = sortRelation.getRelationWriter();

		for (int i = 0; i < numsortedR; i++) {
			inputLoader[i] = sortedSubList.get(i).getRelationLoader();
			inputBuffer[i] = inputLoader[i].loadNextBlocks(1)[0];
			numIO++;
		}

		while (true) {
			// load buffers
			for (int i = 0; i < numsortedR; i++) {
				if ((inputBuffer[i].getNumTuples() == 0) && inputLoader[i].hasNextBlock()) {
					inputBuffer[i] = inputLoader[i].loadNextBlocks(1)[0];
					numIO++;
				}
			}

			// find index of tuple to load
			int index = 0;
			for (int i = 0; i < numsortedR; i++) {
				if (inputBuffer[i].tupleLst.size() > 0){
					index = i;
					break;
				}

			}

			for (int i = index+1; i < numsortedR; i++) {
				if (inputBuffer[i].tupleLst.size() > 0)
					if (inputBuffer[i].tupleLst.get(0).key < inputBuffer[index].tupleLst.get(0).key)
						index = i;
			}

			Tuple minTuple = inputBuffer[index].tupleLst.get(0);
			inputBuffer[index].tupleLst.remove(0);
			if (!outputBuffer.insertTuple(minTuple)){
				rSortWriter.writeBlock(outputBuffer);
				outputBuffer = new Block();
				outputBuffer.insertTuple(minTuple);
			}

			if (sortRelation.getNumTuples() + outputBuffer.getNumTuples() == rel.getNumTuples())
				break;
		}
		rSortWriter.writeBlock(outputBuffer);

		// Write back to original relation
		RelationLoader sortedLoader = sortRelation.getRelationLoader();
		rLoader.reset();

		while(rLoader.hasNextBlock()) {
			Block targetBlock = rLoader.loadNextBlocks(1)[0];
			Block sortedBlock = sortedLoader.loadNextBlocks(1)[0];
			for (int i = 0; i < targetBlock.getNumTuples(); i ++) {
				targetBlock.tupleLst.set(i, sortedBlock.tupleLst.get(i));
			}
			numIO ++;
		}

		rel.printRelation(true, true);

		return numIO;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int hashJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		int M = Setting.memorySize;
		Block[] memBuffers = new Block[M];
		ArrayList<ArrayList<Block>> rBuckets = new ArrayList<ArrayList<Block>>();
		ArrayList<ArrayList<Block>> sBuckets = new ArrayList<ArrayList<Block>>();

		// hash partition
		numIO = partition(relR, memBuffers, rBuckets, M, numIO);
		numIO = partition(relS, memBuffers, sBuckets, M, numIO);

		// join
		Block result = new Block();
		ArrayList<ArrayList<Block>> temp;
		boolean swap = false;
		// the size of the largest bucket is calculated during partition
		// for simplicity, we calculate it seperately here
		int bkSizeR = getLargestBucketSize(rBuckets);
		int bkSizeS = getLargestBucketSize(sBuckets);
		// System.out.println("bkSizeR: " + bkSizeR);
		// System.out.println("bkSizeS: " + bkSizeS);
		if(bkSizeS > M-1){
			if(bkSizeR > M-1){
				System.out.println("Error: One bucket cannot be fully loaded into memory buffer.");
				return 0;
			}
			temp = rBuckets;
			rBuckets = sBuckets;
			sBuckets = temp;
			swap = true;
		}

		// for each bucket of S
		for(int i = 0; i < M-1; i++){
			ArrayList<Block> sBucket = sBuckets.get(i);
			// load the entire bucket to memory
			initMemBuffers(memBuffers, M-1);
			int index = 0;
			for(Block block: sBucket){
				memBuffers[index] = block;
				numIO ++;
				index ++;
			}
			// load each block of R and perform in-memory join
			for(Block block: rBuckets.get(i)){
				memBuffers[M-1] = block;
				numIO ++;
				for(Tuple rTuple: memBuffers[M-1].tupleLst){
					for(int j = 0; j < index; j++){
						for(Tuple sTuple: memBuffers[j].tupleLst){
							if(rTuple.key == sTuple.key){
								JointTuple jt = null;
								if(swap){
									jt = new JointTuple(sTuple, rTuple);
								} else {
									jt = new JointTuple(rTuple, sTuple);
								}
								// if block full, write to disk
								// but we don't count the IO cost here
								while(!result.insertTuple(jt)){
									relRS.getRelationWriter().writeBlock(result);
									result = new Block();
								}
							}
						}
					}
				}
			}
		}
		// write the remaining block to disk
		if(result.tupleLst.size() > 0){
			relRS.getRelationWriter().writeBlock(result);
		}
		
		return numIO;
	}

	private int getTotalTuples(ArrayList<ArrayList<Block>> diskBuckets){
		int num = 0;
		for(ArrayList<Block> bucket: diskBuckets){
			for(Block block: bucket){
				num += block.tupleLst.size();
			}
		}
		return num;
	}

	private void initMemBuffers(Block[] memBuffers, int size){
		for(int i = 0; i < size; i++)
			memBuffers[i] = new Block();
	}

	private void initDiskBuckets(ArrayList<ArrayList<Block>> diskBuckets, int size){
		for(int i = 0; i < size; i++){
			diskBuckets.add(new ArrayList<Block>());
		}
	}


	private int hash(int key, int size){
		int a = 8 * size / 23 + 5;
		return (a * key) % size;
	}

	private int partition(Relation r, Block[] memBuffers, ArrayList<ArrayList<Block>> diskBuckets, int M, int numIO){
		initMemBuffers(memBuffers, M-1);
		initDiskBuckets(diskBuckets, M-1);
		// should be careful, check whether neet to reset "iterator"
		Relation.RelationLoader rLoader = r.getRelationLoader();
		while(rLoader.hasNextBlock()){
			// load next block to the last memory buffer
			memBuffers[M-1] = rLoader.loadNextBlocks(1)[0];
			numIO ++;
			for(Tuple tuple: memBuffers[M-1].tupleLst){
				int id = hash(tuple.key, M-1);
				while(!memBuffers[id].insertTuple(tuple)){
					// if buffer full, write it to disk, empty buffer
					diskBuckets.get(id).add(memBuffers[id]);
					numIO ++;
					memBuffers[id] = new Block();
				}
			}
		}
		// for each buffer, if not empty, write to disk
		for(int i = 0; i < M-1; i++){
			if(memBuffers[i].tupleLst.size() > 0){
				diskBuckets.get(i).add(memBuffers[i]);
				numIO ++;
			}
		}
		// System.out.println("Total Number of tuples in buckets: " + getTotalTuples(diskBuckets));
		return numIO;
	}

	private int getLargestBucketSize(ArrayList<ArrayList<Block>> diskBuckets){
		int maxSize = 0;
		for(ArrayList<Block> bucket: diskBuckets){
			if(bucket.size() > maxSize){
				maxSize = bucket.size();
			}
		}
		return maxSize;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	
	public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;

		// Check enough memory

		RelationLoader rLoader = relR.getRelationLoader();
		RelationLoader sLoader = relS.getRelationLoader();

		ArrayList<Relation> rSubLists = new ArrayList<>();
		ArrayList<Relation> sSubLists = new ArrayList<>();

		// Get R's sub-lists.
		while (rLoader.hasNextBlock()) {
			Relation sortedSubRelation = new Relation("sorted");
			numIO += getSortedSublist(rLoader, sortedSubRelation);
			rSubLists.add(sortedSubRelation);
		}

		// Get S's sub-lists.
		while (sLoader.hasNextBlock()) {
			Relation sortedSubRelation = new Relation("sorted");
			numIO += getSortedSublist(sLoader, sortedSubRelation);
			sSubLists.add(sortedSubRelation);
		}

		// Merge
		int numOfSortedSublists = rSubLists.size();
		RelationLoader[] rSublistLoaders = new RelationLoader[numOfSortedSublists];
		RelationLoader[] sSublistLoaders = new RelationLoader[numOfSortedSublists];
		Block[] rInputBuffers = new Block[numOfSortedSublists];
		Block[] sInputBuffers = new Block[numOfSortedSublists];

		for (int i = 0; i < numOfSortedSublists; i ++) {
			rSublistLoaders[i] = rSubLists.get(i).getRelationLoader();
			sSublistLoaders[i] = sSubLists.get(i).getRelationLoader();
		}

		RelationWriter rsWriter = relRS.getRelationWriter();
		Block rsOutputBuffer = new Block();

		while (true) {
			// Load empty buffers
			numIO += reloadInputBuffers(rSublistLoaders, rInputBuffers);
			numIO += reloadInputBuffers(sSublistLoaders, sInputBuffers);

			// Find smallest items from R & S.
			ArrayList<Tuple> rSmallestTuples = new ArrayList<>();
			ArrayList<Tuple> sSmallestTuples = new ArrayList<>();
			getSmallestTuplesFromSublists(rInputBuffers, rSmallestTuples);
			getSmallestTuplesFromSublists(sInputBuffers, sSmallestTuples);
			if (rSmallestTuples.size() == 0 || sSmallestTuples.size() == 0) break;

			while (sSmallestTuples.get(0).key < rSmallestTuples.get(0).key) {
				sSmallestTuples = new ArrayList<>();
				getSmallestTuplesFromSublists(sInputBuffers, sSmallestTuples);
				if (sSmallestTuples.size() == 0) break;
			}
			if (sSmallestTuples.size() == 0) continue;
			if (sSmallestTuples.get(0).key > rSmallestTuples.get(0).key) continue;

			for (Tuple rTuple : rSmallestTuples) {
				for (Tuple sTuple : sSmallestTuples) {
					Tuple jointTuple = new JointTuple(rTuple, sTuple);
					if (!rsOutputBuffer.insertTuple(jointTuple)) {
						rsWriter.writeBlock(rsOutputBuffer);
						rsOutputBuffer = new Block();
						rsOutputBuffer.insertTuple(jointTuple);
						numIO ++;
					}
				}
			}
		}

		if (rsOutputBuffer.getNumTuples() > 0) {
			rsWriter.writeBlock(rsOutputBuffer);
		}

		return numIO;
	}


	private int getSortedSublist(RelationLoader loader, Relation sublist) {
		int numIO = 0;

		Block[] blocks = loader.loadNextBlocks(Setting.memorySize);

		ArrayList<Tuple> tuples = new ArrayList<Tuple>(Setting.blockFactor * Setting.memorySize);
		for (Block b : blocks) {
			if (b != null) {
				for (Tuple t : b.tupleLst)
					tuples.add(t);
				numIO++;
			}
		}

		Collections.sort(tuples, (t1, t2) -> t1.key - t2.key);

		RelationWriter writer = sublist.getRelationWriter();

		Block sortedBlock = new Block();
		for (Tuple t : tuples) {
			if (!sortedBlock.insertTuple(t)) {
				writer.writeBlock(sortedBlock);
				sortedBlock = new Block();
				sortedBlock.insertTuple(t);
				numIO++;
			}
		}

		if (sortedBlock.getNumTuples() > 0) {
			writer.writeBlock(sortedBlock);
		}

		return numIO;
	}


	private int reloadInputBuffers(RelationLoader[] loaders, Block[] buffers) {
		int numIO = 0;
		for (int i = 0; i < buffers.length; i++) {
			if ((buffers[i] == null || buffers[i].getNumTuples() == 0) && loaders[i].hasNextBlock()) {
				buffers[i] = loaders[i].loadNextBlocks(1)[0];
				numIO ++;
			}
		}
		return numIO;
	}


	private int getSmallestTuplesFromSublists(Block[] buffers, ArrayList<Tuple> tuples) {
		int numIO = 0;
		int smallestKey = Integer.MAX_VALUE;
		for (Block buffer : buffers) {
			for (Tuple tuple : buffer.tupleLst) {
				if (tuple.key < smallestKey) smallestKey = tuple.key;
			}
		}
		for (Block buffer : buffers) {
			Iterator<Tuple> it = buffer.tupleLst.iterator();
			while (it.hasNext()) {
				Tuple tuple = it.next();
				if (tuple.key == smallestKey) {
					tuples.add(tuple);
					it.remove();
				}
			}
		}
		return numIO;
	}

	
	/**
	 * Example usage of classes. 
	 */
	public static void examples(){

		/*Populate relations*/
		System.out.println("---------Populating two relations----------");
		Relation relR=new Relation("RelR");
		int numTuples=relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains "+numTuples+" tuples.");
		Relation relS=new Relation("RelS");
		numTuples=relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains "+numTuples+" tuples.");
		System.out.println("---------Finish populating relations----------\n\n");
			
		/*Print the relation */
		System.out.println("---------Printing relations----------");
		relR.printRelation(true, true);
		relS.printRelation(true, false);
		System.out.println("---------Finish printing relations----------\n\n");
		
		
		/*Example use of RelationLoader*/
		System.out.println("---------Loading relation RelR using RelationLoader----------");
		RelationLoader rLoader=relR.getRelationLoader();		
		while(rLoader.hasNextBlock()){
			System.out.println("--->Load at most 7 blocks each time into memory...");
			Block[] blocks=rLoader.loadNextBlocks(7);
			//print out loaded blocks 
			for(Block b:blocks){
				if(b!=null) b.print(false);
			}
		}
		System.out.println("---------Finish loading relation RelR----------\n\n");
				
		
		/*Example use of RelationWriter*/
		System.out.println("---------Writing to relation RelS----------");
		RelationWriter sWriter=relS.getRelationWriter();
		rLoader.reset();
		if(rLoader.hasNextBlock()){
			System.out.println("Writing the first 7 blocks from RelR to RelS");
			System.out.println("--------Before writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
			
			Block[] blocks=rLoader.loadNextBlocks(7);
			for(Block b:blocks){
				if(b!=null) sWriter.writeBlock(b);
			}
			System.out.println("--------After writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
		}

	}
	
	/**
	 * Testing cases. 
	 */
	public static void testCases(){

		testMergeSortRelation();
//		testHashJoinRelations();
//		testRefinedSortMergeJoinRelations();
	
	}

	public static void testMergeSortRelation() {
		Algorithms algo = new Algorithms();
		Relation relR = new Relation("RelR");
		Relation relS = new Relation("RelS");
		Relation relRS = new Relation("RelRS");
		relR.populateRelationFromFile("RelR.txt");
		relS.populateRelationFromFile("RelS.txt");
		relR.printRelation(true, true);
		relS.printRelation(true, true);
//		algo.mergeSortRelation(relR);
		algo.refinedSortMergeJoinRelations(relR, relS, relRS);
		relRS.printRelation(true, true);
	}

	public static void testHashJoinRelations() {
		Algorithms algo = new Algorithms();

		/*Populate relations*/
		System.out.println("---------Populating two relations----------");
		Relation relR = new Relation("RelR");
		int numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains "+numTuples + " tuples.");
		Relation relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out.println("---------Finish populating relations----------\n\n");

		/*Test Hash Join*/
		Relation relRS = new Relation("RelRS");
		int numIO = algo.hashJoinRelations(relR, relS, relRS);
		System.out.println("---------Hash Join on relR and relS done----------");
		System.out.println("IO cost: " + numIO);
		numTuples = relRS.getNumTuples();
		System.out.println("Relation RelRS contains " + numTuples + " tuples.");

		/*Print the relation */
		System.out.println("---------Printing relation relRS----------");
		relRS.printRelation(true, true);
		// select * FROM relr JOIN rels using (key) ORDER BY key;
	}

	public static void testRefinedSortMergeJoinRelations() {

	}

	/**
	 * This main method provided for testing purpose
	 * @param arg
	 */
	public static void main(String[] arg){
//		Algorithms.examples();
		testCases();
	}
}
