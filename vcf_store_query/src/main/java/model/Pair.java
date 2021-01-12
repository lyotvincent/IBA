package model;

public class Pair {

	public String id1;
	public String id2;
	public float y;
	
	public Pair() {
		super();
	}

	public Pair(float y) {
		super();
		this.y = y;
	}

	public Pair(String id1, String id2, float y) {
		super();
		this.id1 = id1;
		this.id2 = id2;
		this.y = y;
	}
	
}
