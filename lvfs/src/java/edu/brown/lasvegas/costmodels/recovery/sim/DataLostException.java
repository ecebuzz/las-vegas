package edu.brown.lasvegas.costmodels.recovery.sim;

public class DataLostException extends Exception {
	private static final long serialVersionUID = 0;
	public DataLostException (String message) {
		super (message);
	}
}