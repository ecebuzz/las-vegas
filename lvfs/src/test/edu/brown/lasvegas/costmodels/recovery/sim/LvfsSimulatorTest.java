package edu.brown.lasvegas.costmodels.recovery.sim;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import edu.brown.lasvegas.costmodels.recovery.sim.Simulator.SimulationResult;

/**
 * Testcase for {@link LvfsSimulator}.
 */
public class LvfsSimulatorTest {
	private ExperimentalConfiguration config;
	@Before
	public void setup () {
		// relatively small setting for faster tests.
		config = new ExperimentalConfiguration(10,10,10,10,
				4.3d * 30 * 24 * 60, 10.2d * 365 * 24 * 60,
				0.05d * 60, 0.02d * 60, 3.0d * 60, 0.1d * 60,
				365.0d * 24 * 60);
	}
	@Test
	public void testRepFac1 () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{1}, 10, false, false, false),
				3311);
		simulator.decidePlacement();
		SimulationResult results = simulator.simulateMeanTimeToFail(10);
		for (Double time : results.getResults()) {
			assertTrue(!time.isInfinite()); // replication factor 1 should immediately fail.
		}
	}

	@Test
	public void testRepFac2 () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{2}, 10, false, false, false),
				3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
	}
	@Test
	public void testRepFac3 () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{3}, 10, false, false, false),
				3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
	}
	@Test
	public void testRepFac3BuddyEx () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{3}, 10, true, false, false),
				3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
	}
	@Test
	public void testRepFac3BuddyExNodeCp () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{3}, 10, true, true, false),
				3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
	}
	@Test
	public void testRepFac3All () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{3}, 10, true, true, true),
				3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
	}
	@Test
	public void testRepFac12All () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{1, 2}, 4, true, true, true),
				3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
	}
	@Test
	public void testRepFac111All () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{1, 1, 1}, 4, true, true, true),
				3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
	}
	@Test
	public void testRepFac22All () {
		LvfsSimulator simulator = new LvfsSimulator(config,
				new LvfsPlacementParameters(10, new int[]{2, 2}, 4, true, true, true),
				3311);
		simulator.decidePlacement();
		simulator.simulateMeanTimeToFail(10);
	}
}
