#!/bin/sh
# execute from project folder (lvfs)
run_read_cacheclear() {
	for rep in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
	do
		sudo sync
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		sleep 1
		java -classpath "bin:lib/*" edu.brown.lasvegas.lvfs.local.ReaderWriterBenchmark readonly
	done
	echo "run_read_cacheclear done"
}
run_read() {
	java -classpath "bin:lib/*" edu.brown.lasvegas.lvfs.local.ReaderWriterBenchmark readonly
	echo "warmed cache. ignore the result above."
	for rep in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
	do
		java -classpath "bin:lib/*" edu.brown.lasvegas.lvfs.local.ReaderWriterBenchmark readonly
	done
	echo "run_read done"
}
run_write() {
	for rep in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
	do
		java -classpath "bin:lib/*" edu.brown.lasvegas.lvfs.local.ReaderWriterBenchmark writeonly
	done
	echo "run_write done"
}

run_write
run_read_cacheclear
run_read
