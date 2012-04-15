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

	private void test (int replicationFactor, boolean secondReplicaInSameRack, boolean stripeChunking) {
		HdfsSimulator simulator = new HdfsSimulator(config, new HdfsPlacementParameters(replicationFactor, secondReplicaInSameRack, stripeChunking), 3311);
		simulator.decidePlacement();
		SimulationResult results = simulator.simulateMeanTimeToFail(10);
		if (replicationFactor == 1) {
			for (double time : results.getResults() ) {
				assertTrue(time != Double.POSITIVE_INFINITY); // replication factor 1 should immediately fail.
			}
		}
		// replication factor 2,3 should be okay (for this number of node/rack). but not certainly.
	}
	
	@Test
	public void testRepFac1 () { test (1, true, false); }
	@Test
	public void testRepFac1Rack () { test (1, false, false); }
	@Test
	public void testRepFac1RackStripe () { test (1, false, true); }
	@Test
	public void testRepFac2 () { test (2, true, false); }
	@Test
	public void testRepFac2Rack () { test (2, false, false); }
	@Test
	public void testRepFac2RackStripe () { test (2, false, true); }
	@Test
	public void testRepFac3 () { test (3, true, false); }
	@Test
	public void testRepFac3Rack () { test (3, false, false); }
	@Test
	public void testRepFac3RackStripe () { test (3, false, true); }
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
