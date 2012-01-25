package edu.brown.lasvegas.lvfs.data;

import edu.brown.lasvegas.server.StandaloneCentralNode;

/**
 * A performance benchmark program to test large data import
 * in multiple nodes on top of a real HDFS cluster.
 * This is NOT a testcase.
 * 
 * <p>This one has to run on a real distributed HDFS cluster,
 * so you need to follow the following steps to run this program.
 * There are a few scripts in src/script to help you. But, still
 * many of the steps are inherently manual (or at least you have to adjust
 * something for your environment).
 * </p>
 * 
 * <h2>Step 1</h2>
 * <p>
 * Download and compile the source code in <b>each</b> machine.
 * <pre>
 * git clone git://github.com/hkimura/las-vegas.git
 * cd las-vegas/lvfs
 * ant
 * </pre>
 * </p>
 * 
 * <h2>Step 2</h2>
 * <p>
 * Configure an xml file. Examples are in src/test/edu/brown/lasvegas/server/lvfs_conf_xxx.xml.
 * At least modify the followings:
 * lasvegas.server.data.address,
 * lasvegas.server.data.node_name,
 * lasvegas.server.data.rack_name
 * </p>
 * 
 * <h2>Step 3</h2>
 * <p>
 * Launch the central node in a single machine.
 * <pre>
 * ant -Dconfxml=lvfs_conf_poseidon.xml -Dformat=true sa-central
 * </pre>
 * -Dformat is the parameter to specify whether to nuke the metadata repository.
 * In order to stop:  
 * <pre>
 * ant -Dconfxml=lvfs_conf_poseidon.xml sa-central-stop
 * </pre>
 * </p>
 * 
 * <h2>Step 4</h2>
 * <p>
 * Launch the data node in each machine.
 * <pre>
 * ant -Dconfxml=edu.brown.lasvegas.server.lvfs_conf_poseidon.xml -Dformat=true sa-data
 * </pre>
 * -Dformat is the parameter to specify whether to nuke the data folder.
 * In order to stop:  
 * <pre>
 * ant -Dconfxml=lvfs_conf_poseidon.xml -Daddress=poseidon:28712 sa-data-stop
 * </pre>
 * </p>
 */
public class DataImportMultiNodeBenchmark {

}
