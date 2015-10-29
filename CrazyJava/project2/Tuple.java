/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2.
 * This class shall not be modified in any form. 
 */

package project2;

public class Tuple {
	
	/*The value of the attribute used for sorting a relation or joining two relations*/
	protected int key;  
	
	/*Value of all remaining attributes in this tuple is represented by a long string for simplicity*/
	protected String value; 
	
	public Tuple(int key, String value) {
		this.key = key;
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "Tuple [Key=" + key + ",\tValue=" + value + "]";
	}

}
