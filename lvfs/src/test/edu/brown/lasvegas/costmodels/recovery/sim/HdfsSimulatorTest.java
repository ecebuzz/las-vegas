package edu.brown.lasvegas.costmodels.recovery.sim;

import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.costmodels.recovery.sim.Simulator.SimulationResult;
import static org.junit.Assert.*;

/**
 * Testcase for {@link HdfsSimulator}.
 */
public class HdfsSimulatorTest {
	private ExperimentalConfiguration config;
	@Before
	public void setup () {
		// relatively small setting for faster tests.
		config = new ExperimentalConfiguration(10,10,100,10,
				4.3d * 30 * 24 * 60, 10.2d * 365 * 24 * 60,
				0.05d * 60, 0.02d * 60, 3.0d * 60, 0.1d * 60,
				365.0d * 24 * 60);
	}
	
	@Test
	public void testRepFac1 () {
		HdfsSimulator simulator = new HdfsSimulator(config, new HdfsPlacementParameters(1), 3311);
		simulator.decidePlacement();
		SimulationResult results = simulator.simulateMeanTimeToFail(10);
		for (Double time : results.getResults()) {
			assertTrue(!time.isInfinite()); // replication factor 1 should immediately fail.
		}
	}
	@Test
	public void testRepFac2 () {
		HdfsSimulator simulator = new HdfsSimulator(config, new HdfsPlacementParameters(2), 3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
		// replication factor 2 will also see data loss, but not certainly.
	}
	@Test
	public void testRepFac3 () {
		HdfsSimulator simulator = new HdfsSimulator(config, new HdfsPlacementParameters(3), 3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
		// replication factor 3 should be okay (for this number of node/rack). but not certainly either.
	}
/* this test takes time.
	@Test
	public void testRepFac3Larger () {
		config = new ExperimentalConfiguration(100,100,100,100,
				4.3d * 30 * 24 * 60, 10.2d * 365 * 24 * 60,
				0.05d * 60, 3.0d * 60, 0.1d * 60,
				365.0d * 24 * 60);
		HdfsSimulator simulator = new HdfsSimulator(config, new HdfsPlacementPolicy(3), 3311);
		simulator.decidePlacement();
		ArrayList<Double> results = simulator.simulateMeanTimeToFail(5);
	}
	*/
}
