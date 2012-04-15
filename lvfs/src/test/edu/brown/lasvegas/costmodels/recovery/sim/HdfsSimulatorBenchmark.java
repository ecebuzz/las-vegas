package edu.brown.lasvegas.costmodels.recovery.sim;

/**
 * Experiments with HdfsSimulator.
 * This is NOT a testcase.
 */
public class HdfsSimulatorBenchmark {
	public static void main (String[] args) {
		for (int rep = 1; rep <= 4; ++rep) {
			run (new HdfsPlacementParameters(rep, true, false));
			run (new HdfsPlacementParameters(rep, true, true));
			run (new HdfsPlacementParameters(rep, false, false));
			run (new HdfsPlacementParameters(rep, false, true));
		}
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
