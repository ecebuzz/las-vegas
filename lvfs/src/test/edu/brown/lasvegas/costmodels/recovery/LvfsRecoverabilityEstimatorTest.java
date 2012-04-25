package edu.brown.lasvegas.costmodels.recovery;

import org.apache.log4j.Logger;
import org.junit.Test;

import edu.brown.lasvegas.costmodels.recovery.sim.ExperimentalConfiguration;
import edu.brown.lasvegas.costmodels.recovery.sim.LvfsPlacementParameters;
import edu.brown.lasvegas.costmodels.recovery.sim.LvfsSimulatorBenchmark;

/**
 * Testcase and also experiments for {@link LvfsRecoverabilityEstimator}.
 * It's fast, so why not make it a testcase too.
 */
public class LvfsRecoverabilityEstimatorTest {
    private static Logger LOG = Logger.getLogger(LvfsRecoverabilityEstimatorTest.class);
    @Test
	public void test() {
		run (new LvfsPlacementParameters(1, new int[]{1, 1}, 10, true, true, true));
		run (new LvfsPlacementParameters(2, new int[]{1, 1}, 5, true, true, true));
		run (new LvfsPlacementParameters(5, new int[]{1, 1}, 2, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 1}, 1, true, true, true));
		run (new LvfsPlacementParameters(1, new int[]{2}, 20, true, true, true));
		run (new LvfsPlacementParameters(2, new int[]{2}, 10, true, true, true));
		run (new LvfsPlacementParameters(5, new int[]{2}, 4, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{2}, 2, true, true, true));
		run (new LvfsPlacementParameters(1, new int[]{1, 1, 1}, 10, true, true, true));
		run (new LvfsPlacementParameters(2, new int[]{1, 1, 1}, 5, true, true, true));
		run (new LvfsPlacementParameters(5, new int[]{1, 1, 1}, 2, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 1, 1}, 1, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 2}, 2, true, true, true));
		run (new LvfsPlacementParameters(1, new int[]{3}, 30, true, true, true));
		run (new LvfsPlacementParameters(2, new int[]{3}, 15, true, true, true));
		run (new LvfsPlacementParameters(5, new int[]{3}, 6, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{3}, 3, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{4}, 4, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{2, 2}, 2, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 1, 2}, 2, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 3}, 3, true, true, true));
		run (new LvfsPlacementParameters(10, new int[]{1, 1, 1, 1}, 1, true, true, true));
	}
	
	private static void run (LvfsPlacementParameters parameters) {
		ExperimentalConfiguration config = LvfsSimulatorBenchmark.createConfig();
		LvfsRecoverabilityEstimator estimator = new LvfsRecoverabilityEstimator(config, parameters);
		double count = estimator.estimateFailureCount();
		double mttf = config.maxSimulationPeriod / count;
		double log10Mttf = Math.log10(mttf);
		LOG.info("param={" + parameters + "}, count=" + count + ", mean=" + mttf + ", log10mean=" + log10Mttf);
	}

}
