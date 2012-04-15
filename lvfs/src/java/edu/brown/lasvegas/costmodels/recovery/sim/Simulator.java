package edu.brown.lasvegas.costmodels.recovery.sim;

import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;

public abstract class Simulator {
    private static Logger LOG = Logger.getLogger(Simulator.class);
	public Simulator(ExperimentalConfiguration config, long firstRandomSeed) {
		this.config = config;
		this.firstRandomSeed = firstRandomSeed;
	}
	protected final ExperimentalConfiguration config;
	protected final long firstRandomSeed;

	public abstract void decidePlacement ();

	/** simulate one failure schedule and returns the fail time with the schedule. +Infinity when max-simulation-period elapses. */
	protected abstract double simulateTimeToFail (FailureSchedule schedule);

	public SimulationResult simulateMeanTimeToFail (int iterations) {
		LOG.info("Started " + iterations + " iterations");
		ArrayList<Double> results = new ArrayList<Double>();
		Random seedGenerator = new Random(firstRandomSeed);
		long start = System.currentTimeMillis();
		long loggedPrevious = start;
		for (int i = 0; i < iterations; ++i) {
			if (System.currentTimeMillis() > loggedPrevious + 10000) {
				loggedPrevious = System.currentTimeMillis();
				LOG.info(i + "/" + iterations + "... (" + ((loggedPrevious - start)/1000.0d) + " sec elapsed)");
			}
			long seed = seedGenerator.nextLong();
			// LOG.info("seed=" + seed);
			FailureSchedule schedule = new FailureSchedule(config, seed);
			results.add(simulateTimeToFail(schedule));
		}
		SimulationResult result = new SimulationResult(results);
		LOG.info("Done iterations:config={" + config + "}, param={" + summarizeParameters() + "}, result=" + result);
		return result;
	}
	protected abstract String summarizeParameters();
	
	public class SimulationResult {
		public SimulationResult(ArrayList<Double> list) {
			final int n = list.size();
			results = new double[n];
			double sum = 0, sqsum = 0, log10Sum = 0, log10Sqsum = 0;
			noFailureCount = 0;
			max = 0;
			min = Double.MAX_VALUE;
			for (int i = 0; i < n; ++i) {
				double time = list.get(i);
				if (time == Double.POSITIVE_INFINITY) {
					time = config.maxSimulationPeriod;
					++noFailureCount; // this affects mean/stdev/max
				}
				results[i] = time;
				if (time > max) {
					max = time;
				}
				if (time < min) {
					min = time;
				}
				sum += time;
				sqsum += time * time;
				log10Sum += Math.log10(time);
				log10Sqsum += Math.log10(time) * Math.log10(time);
			}
			mean = sum / n;
			log10Mean = log10Sum / n;
			// this is an estimation from samples, so use N-1 to be unbiased.
			stdev = Math.sqrt((sqsum + mean * mean * n - 2.0d * n * mean * mean) / (n - 1));
			log10Stdev = Math.sqrt((log10Sqsum + log10Mean * log10Mean * n - 2.0d * n * log10Mean * log10Mean) / (n - 1));
		}
		private double[] results;
		private double mean, log10Mean;
		private double stdev, log10Stdev;
		private double max;
		private double min;
		private int noFailureCount;
		public double[] getResults() {
			return results;
		}
		public double getMean() {
			if (noFailureCount > 0) {
				LOG.warn("One of the schedules had no failure! the mean assumes"
						+ config.maxSimulationPeriod + " minutes as the failure time, but this is not accurate!!!");
			}
			return mean;
		}
		public double getLog10Mean() {
			if (noFailureCount > 0) {
				LOG.warn("One of the schedules had no failure! the mean assumes"
						+ config.maxSimulationPeriod + " minutes as the failure time, but this is not accurate!!!");
			}
			return log10Mean;
		}
		public double getStdev() {
			if (noFailureCount > 0) {
				LOG.warn("One of the schedules had no failure! the stdev assumes"
						+ config.maxSimulationPeriod + " minutes as the failure time, but this is not accurate!!!");
			}
			return log10Stdev;
		}
		public double getLog10Stdev() {
			if (noFailureCount > 0) {
				LOG.warn("One of the schedules had no failure! the stdev assumes"
						+ config.maxSimulationPeriod + " minutes as the failure time, but this is not accurate!!!");
			}
			return stdev;
		}
		public double getMax() {
			if (noFailureCount > 0) {
				LOG.warn("One of the schedules had no failure! the max assumes"
						+ config.maxSimulationPeriod + " minutes as the failure time, but this is not accurate!!!");
			}
			return max;
		}
		public double getMin() {
			return min;
		}
		public int getNoFailureCount() {
			return noFailureCount;
		}
		@Override
		public String toString() {
			String str = "Results (in minutes)=[";
			for (int i = 0; i < results.length; ++i) {
				if (i != 0) {
					str += ",";
				}
				str += results[i];
			}
			str += "]";
			str += ", mean=" + mean + ", log10Mean=" + log10Mean + ", stdev=" + stdev + ", log10Stdev=" + log10Stdev + ", max=" + max + ", min=" + min + ", noFailureCount=" + noFailureCount;
			return str;
		}
	}
}

