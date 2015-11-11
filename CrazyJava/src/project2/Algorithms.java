/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

package project2;

import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;

import java.lang.reflect.Array;
import java.util.*;

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
		ArrayList<Relation> sortedSubList = new ArrayList<>();

		while (rLoader.hasNextBlock()) {
			Relation sortedSubRelation = new Relation("sorted");
			numIO += getSortedSublist(rLoader, sortedSubRelation);
			sortedSubList.add(sortedSubRelation);
		}


		// Phase 2: merge
		int numOfSortedSublists = sortedSubList.size();

		Block[] inputBuffer = new Block[numOfSortedSublists];
		RelationLoader[] inputLoader = new RelationLoader[numOfSortedSublists];
		Block outputBuffer = new Block();

		rLoader.reset();

		for (int i = 0; i < numOfSortedSublists; i++) {
			inputLoader[i] = sortedSubList.get(i).getRelationLoader();
		}

		while (true) {
			numIO += reloadInputBuffers(inputLoader, inputBuffer);

			ArrayList<Tuple> smallestTuples = new ArrayList<>();
			getSmallestTuplesFromSublists(inputBuffer, smallestTuples);

			if (smallestTuples.size() == 0) break;

			for (Tuple tuple : smallestTuples) {
				if (!outputBuffer.insertTuple(tuple)) {
					Block targetBlock = rLoader.loadNextBlocks(1)[0];
					targetBlock.tupleLst = outputBuffer.tupleLst;
					outputBuffer = new Block();
					outputBuffer.insertTuple(tuple);
					numIO ++;
				}
			}

		}
		if (outputBuffer.getNumTuples() > 0) {
			Block targetBlock = rLoader.loadNextBlocks(1)[0];
			targetBlock.tupleLst = outputBuffer.tupleLst;
			numIO ++;
		}

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
		RelationLoader[] rSublistLoaders = new RelationLoader[rSubLists.size()];
		RelationLoader[] sSublistLoaders = new RelationLoader[sSubLists.size()];
		Block[] rInputBuffers = new Block[rSubLists.size()];
		Block[] sInputBuffers = new Block[sSubLists.size()];

		for (int i = 0; i < rSubLists.size(); i ++) {
			rSublistLoaders[i] = rSubLists.get(i).getRelationLoader();
		}
		for (int i = 0; i < sSubLists.size(); i ++) {
			sSublistLoaders[i] = sSubLists.get(i).getRelationLoader();
		}

		RelationWriter rsWriter = relRS.getRelationWriter();
		Block rsOutputBuffer = new Block();

		ArrayList<Tuple> rSmallestTuples = new ArrayList<>();
		ArrayList<Tuple> sSmallestTuples = new ArrayList<>();

		while (true) {
			// Load empty buffers
			numIO += reloadInputBuffers(rSublistLoaders, rInputBuffers);
			numIO += reloadInputBuffers(sSublistLoaders, sInputBuffers);

			// Find smallest items from R & S.
			if (rSmallestTuples.size() == 0) {
				getSmallestTuplesFromSublists(rInputBuffers, rSmallestTuples);
			}
			if (sSmallestTuples.size() == 0) {
				getSmallestTuplesFromSublists(sInputBuffers, sSmallestTuples);
			}
			if (rSmallestTuples.size() == 0 || sSmallestTuples.size() == 0) break;

			// Continue on reload if representative group has smaller keys.
			if (rSmallestTuples.get(0).key < sSmallestTuples.get(0).key) {
				rSmallestTuples.clear();
				continue;
			}
			if (rSmallestTuples.get(0).key > sSmallestTuples.get(0).key) {
				sSmallestTuples.clear();
				continue;
			}

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

			rSmallestTuples.clear();
			sSmallestTuples.clear();
		}

		if (rsOutputBuffer.getNumTuples() > 0) {
			rsWriter.writeBlock(rsOutputBuffer);
			numIO ++;
		}

		return numIO;
	}


	private int getSortedSublist(RelationLoader loader, Relation sublist) {
		int numIO = 0;

		Block[] blocks = loader.loadNextBlocks(Setting.memorySize);

		ArrayList<Tuple> tuples = new ArrayList<>();
		for (Block b : blocks) {
			if (b != null) {
				for (Tuple t : b.tupleLst) tuples.add(t);
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
			numIO ++;
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
			if (buffer.getNumTuples() > 0 && buffer.tupleLst.get(0).key < smallestKey)
				smallestKey = buffer.tupleLst.get(0).key ;
		}
		for (Block buffer : buffers) {
			Iterator<Tuple> it = buffer.tupleLst.iterator();
			while (it.hasNext()) {
				Tuple tuple = it.next();
				if (tuple.key == smallestKey) {
					tuples.add(tuple);
					it.remove();
				} else {
					break;
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

//		testMergeSortRelation();
//		testHashJoinRelations();
		testRefinedSortMergeJoinRelations();
	
	}

	private static void compareRelation(Relation a, Relation b) {
		if (a.getNumTuples() != b.getNumTuples()) {
			System.out.println("[ERROR] Different numbers of tuples. A: " + a.getNumTuples() + " B: " + b.getNumTuples());
//			return;
		}
		System.out.println("Both relation have " + a.getNumTuples() + " tuples.");
		RelationLoader loaderA = a.getRelationLoader();
		RelationLoader loaderB = b.getRelationLoader();

		ArrayList<Tuple> tuplesA = new ArrayList<>();
		ArrayList<Tuple> tuplesB = new ArrayList<>();

		while (loaderA.hasNextBlock()) {
			Block blockA = loaderA.loadNextBlocks(1)[0];
			Block blockB = loaderB.loadNextBlocks(1)[0];

			tuplesA.addAll(blockA.tupleLst);
			tuplesB.addAll(blockB.tupleLst);
		}

		for (int i = 0; i < tuplesA.size(); i ++) {
			HashSet<String> valuesFromA = new HashSet<>();
			HashSet<String> valuesFromB = new HashSet<>();
			valuesFromA.add(tuplesA.get(i).value);
			for (int j = i + 1; j < tuplesA.size(); j ++) {
				if (tuplesA.get(j).key == tuplesA.get(i).key) {
					valuesFromA.add(tuplesA.get(j).value);
				} else break;
			}
			for (int j = i; j < tuplesB.size(); j ++) {
				if (tuplesB.get(j).key == tuplesA.get(i).key) {
					valuesFromB.add(tuplesB.get(j).value);
				} else break;
			}
			if (!valuesFromA.equals(valuesFromB)) {
				System.out.println("[ERROR] Tuples are different on key " + tuplesA.get(i).key);
				System.out.println("A: " + valuesFromA.toString());
				System.out.println("B: " + valuesFromB.toString());
			}
			i += valuesFromA.size() - 1;
		}

		System.out.println("All tuples are the same.");
	}

	public static void testMergeSortRelation() {
		System.out.println("Test merge sort relations.");

		Algorithms algo = new Algorithms();

		Relation relR = new Relation("RelR");
		Relation relRSorted = new Relation("RelRSorted");
		relR.populateRelationFromFile("RelR.txt");
		relRSorted.populateRelationFromFile("RelRSorted.txt");
		algo.mergeSortRelation(relR);

		Relation relS = new Relation("RelS");
		Relation relSSorted = new Relation("RelSSorted");
		relS.populateRelationFromFile("RelS.txt");
		relSSorted.populateRelationFromFile("RelSSorted.txt");
		algo.mergeSortRelation(relS);

		System.out.println("Verifying merge sort on relation R.");
		compareRelation(relR, relRSorted);
		System.out.println("Verifying merge sort on relation S.");
		compareRelation(relS, relSSorted);
	}

	public static void testHashJoinRelations() {
		Algorithms algo = new Algorithms();
		Relation relR = new Relation("RelR");
		Relation relS = new Relation("RelS");
		Relation relRS = new Relation("RelRS");
		Relation relJoint = new Relation("RelJoint");
		relR.populateRelationFromFile("RelR.txt");
		relS.populateRelationFromFile("RelS.txt");
		relJoint.populateRelationFromFile("RelJoint.txt");
		algo.hashJoinRelations(relR, relS, relRS);
		algo.mergeSortRelation(relRS);
		compareRelation(relRS, relJoint);
	}

	public static void testRefinedSortMergeJoinRelations() {
		Algorithms algo = new Algorithms();
		Relation relR = new Relation("RelR");
		Relation relS = new Relation("RelS");
		Relation relRS = new Relation("RelRS");
		Relation relJoint = new Relation("RelJoint");
		relR.populateRelationFromFile("RelR.txt");
		relS.populateRelationFromFile("RelS.txt");
		relJoint.populateRelationFromFile("RelJoint.txt");
		algo.refinedSortMergeJoinRelations(relR, relS, relRS);
		compareRelation(relRS, relJoint);
		relRS.printRelation(true, true);
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
