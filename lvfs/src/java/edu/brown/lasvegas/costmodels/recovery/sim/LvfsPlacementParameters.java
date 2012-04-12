package edu.brown.lasvegas.costmodels.recovery.sim;

/**
 * Parameters for LVFS data placement.
 * These parameters are much more simplified than
 * the real metadata repository because we just need
 * an approximate failure rate.
 */
public class LvfsPlacementParameters {
	public LvfsPlacementParameters(int fracturesPerTable,
			int[] replicaSchemes,
			int racksPerGroup,
			boolean buddyExclusion, boolean nodeCoupling, boolean buddySwapping) {
		this.fracturesPerTable = fracturesPerTable;
		this.replicaSchemes = replicaSchemes.clone();
		this.racksPerGroup = racksPerGroup;
		this.buddyExclusion = buddyExclusion;
		this.nodeCoupling = nodeCoupling;
		this.buddySwapping = buddySwapping;
	}
	
	/** number of fractures in each table. */
	public final int fracturesPerTable;
	/**
	 * number of replica schemes in each replica group.
	 * eg, {1,1,1}=3 non-buddy replicas. {2}=2 buddy replicas.
	 * {2,3}=a replica group with 2 buddy replicas and a replica group with 3 buddy replicas. 
	 */
	public final int[] replicaSchemes;
	
	/**
	 * number of racks dedicated to a replica group in a fracture. 
	 * should be more than 1 for node coupling. 
	 */
	public final int racksPerGroup;
	
	/** whether to do buddy exclusion. */
	public final boolean buddyExclusion;
	/** whether to do node coupling. */
	public final boolean nodeCoupling;
	/** whether to do buddy swapping. */
	public final boolean buddySwapping;
	
	@Override
	public String toString() {
		String str = "replicaSchemes=[";
		for (int i = 0; i < replicaSchemes.length; ++i) {
			if (i != 0) {
				str += ",";
			}
			str += replicaSchemes[i];
		}
		str += "], fracturesPerTable=" + fracturesPerTable;
		str += ", racksPerGroup=" + racksPerGroup;
		str += ", buddyExclusion=" + buddyExclusion;
		str += ", nodeCoupling=" + nodeCoupling;
		str += ", buddySwapping=" + buddySwapping;
		return str;
	}
}
