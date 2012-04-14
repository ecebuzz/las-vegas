package edu.brown.lasvegas.costmodels.recovery.sim;

/**
 * Experiments with HdfsSimulator.
 * This is NOT a testcase.
 */
public class HdfsSimulatorBenchmark {
	public static void main (String[] args) {
		run (new HdfsPlacementParameters(1, true));
		run (new HdfsPlacementParameters(1, false));
		run (new HdfsPlacementParameters(2, true));
		run (new HdfsPlacementParameters(2, false));
		run (new HdfsPlacementParameters(3, true));
		run (new HdfsPlacementParameters(3, false));
		run (new HdfsPlacementParameters(4, true));
		run (new HdfsPlacementParameters(4, false));
	}
	
	private static ExperimentalConfiguration createConfig() {
		return LvfsSimulatorBenchmark.createConfig();
	}
	
	private static void run (HdfsPlacementParameters parameters) {
		HdfsSimulator simulator = new HdfsSimulator(createConfig(), parameters, 3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(100);
	}
}
