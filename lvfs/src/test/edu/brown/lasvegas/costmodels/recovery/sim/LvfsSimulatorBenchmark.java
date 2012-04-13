package edu.brown.lasvegas.costmodels.recovery.sim;

/**
 * Experiments with LvfsSimulator.
 * This is NOT a testcase.
 */
public class LvfsSimulatorBenchmark {
	public static void main (String[] args) {
		run (new LvfsPlacementParameters(1, new int[]{1, 1}, 50, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 1}, 2, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{2}, 4, true, true, true));
		run (new LvfsPlacementParameters(1, new int[]{1, 1, 1}, 33, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 1, 1}, 2, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 2}, 4, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{3}, 6, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{2, 2}, 4, true, true, true));
	}
	
	private static ExperimentalConfiguration createConfig() {
		return new ExperimentalConfiguration(100,100,100,100,
			4.3d * 30 * 24 * 60, 10.2d * 365 * 24 * 60,
			0.05d * 60, 0.02d * 60, 3.0d * 60, 0.1d * 60,
			3650.0d * 24 * 60);
	}
	
	private static void run (LvfsPlacementParameters parameters) {
		LvfsSimulator simulator = new LvfsSimulator(createConfig(), parameters, 3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(100);
	}

}
