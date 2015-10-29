/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2.
 * This class shall not be modified in any form. 
 */

package project2;

public class JointTuple extends Tuple{
	/**
	 * This is a tuple in a result join relation. 
	 * Note that only one key value is necessary because tr.key==ts.key (i.e., the joint attribute)
	 * @param tr a tuple from of the relations in the join
	 * @param ts a matching tuple from the other relation in the join
	 */
	public JointTuple(Tuple tr, Tuple ts) {
		super(tr.key, tr.value+"_"+ts.value);
		if(tr.key!=ts.key){
			System.err.println("A joint tuple constructed with mismatching tuples"+tr+ts);
		}
		
	}

}
