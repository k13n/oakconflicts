package org.k13n.oakconflicts.benchmark;

public class Benchmark2 {

    public static void main(String[] args) {
        int clusterSize = 2;
        for (int nrWorkers = 1; nrWorkers <= 10; ++nrWorkers) {
            new Benchmark(clusterSize, nrWorkers).execute();
            System.out.println("\n\n\n\n\n\n\n");
        }
    }

}
