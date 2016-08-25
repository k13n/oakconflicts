package org.k13n.oakconflicts.benchmark;

public class Benchmark1 {

    public static void main(String[] args) {
        int clusterSize = 1;
        for (int nrWorkers = 1; nrWorkers <= 10; ++nrWorkers) {
            new Benchmark(clusterSize, nrWorkers).execute();
        }
    }

}
