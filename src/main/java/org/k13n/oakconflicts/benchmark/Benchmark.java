package org.k13n.oakconflicts.benchmark;

import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.document.Commit;
import org.k13n.oakconflicts.ClusterNode;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Benchmark {
    private static final String DB_NAME = "conflicts";
    private static final String PARENT_NAME = "test";
    private static final String PROPERTY_NAME = "property";
    private static final String PROPERTY_VALUE = "value";

    private final DescriptiveStatistics globalStats;
    private final ClusterNode[] cluster;
    private final Worker[] workers;
    private LinkedList<ClusterNode> freeClusterNodes;

    public Benchmark(int clusterSize, int nrWorkers) {
        globalStats = new DescriptiveStatistics();
        cluster = new ClusterNode[clusterSize];
        workers = new Worker[nrWorkers];
        freeClusterNodes = new LinkedList<>();
    }

    public void execute() {
        setUpExperiment();
        runExperiment();
        concludeExperiment();
    }

    private void setUpExperiment() {
        ClusterNode.dropDatabase(DB_NAME);
        ClusterNode setUpClusterNode = new ClusterNode(DB_NAME, 1);
        setUpPropertyIndex(setUpClusterNode);

        try (ContentSession session = setUpClusterNode.newSession()) {
            Root rootTree = session.getLatestRoot();
            Tree parent = rootTree.getTree("/");
            parent.addChild(PARENT_NAME);
            rootTree.commit();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CommitFailedException e) {
            e.printStackTrace();
        } catch (LoginException e) {
            e.printStackTrace();
        } catch (NoSuchWorkspaceException e) {
            e.printStackTrace();
        }

        setUpClusterNode.tearDownRepository();

        for (int i = 0; i < cluster.length; ++i) {
            cluster[i] = new ClusterNode(DB_NAME, i+10);
            freeClusterNodes.add(cluster[i]);
        }

        for (int i = 0; i < workers.length; ++i) {
            ClusterNode clusterNode;
            if (!freeClusterNodes.isEmpty()) {
                clusterNode = freeClusterNodes.removeFirst();
            } else {
                clusterNode = cluster[ThreadLocalRandom.current().nextInt(0, cluster.length)];
            }

            Worker worker = new Worker(i, clusterNode);
            worker.setUp();

            workers[i] = worker;
        }

        Commit.conflictCounter.set(0);
    }

    private void setUpPropertyIndex(ClusterNode clusterNode) {
        try (ContentSession session = clusterNode.newSession()) {
            Root rootTree = session.getLatestRoot();
            Tree parent = rootTree.getTree("/oak:index");

            List<String> propertyNames = ImmutableList.of(PROPERTY_NAME);

            Tree index = parent.addChild(PROPERTY_NAME);
            index.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
            index.setProperty("type", "property");
            index.setProperty("propertyNames", propertyNames, Type.NAMES);
            index.setProperty("unique", false);
            index.setProperty("reindex", true);

            rootTree.commit();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (CommitFailedException e) {
            e.printStackTrace();
        } catch (LoginException e) {
            e.printStackTrace();
        } catch (NoSuchWorkspaceException e) {
            e.printStackTrace();
        }
    }

    private void runExperiment() {
        for (int i = 0; i < workers.length; ++i) {
            workers[i].start();
        }

        try {
            Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < workers.length; ++i) {
            workers[i].interrupted = true;
        }
        for (int i = 0; i < workers.length; ++i) {
            try {
                workers[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void concludeExperiment() {
        System.out.println("cluster size: " + cluster.length);
        System.out.println("nr workers: " + workers.length);
        System.out.println("N: " + globalStats.getN());
        System.out.println("min: " + globalStats.getMin());
        System.out.println("p10: " + globalStats.getPercentile(10.0));
        System.out.println("p50: " + globalStats.getPercentile(50.0));
        System.out.println("p90: " + globalStats.getPercentile(90.0));
        System.out.println("p99: " + globalStats.getPercentile(99.0));
        System.out.println("max: " + globalStats.getMax());
        System.out.println("#conflicts: " + Commit.conflictCounter.get());
        System.out.println();
        for (int i = 0; i < workers.length; ++i) {
            Worker worker = workers[i];
            System.out.println(worker);
            System.out.println("commits: " + worker.getTnxCommits());
            System.out.println("aborts:  " + worker.getTnxAborts());
            System.out.println("avgRuntime: " + worker.getLocalStats().getMean());
        }
    }

    public class Worker extends Thread {
        private final DescriptiveStatistics localStats;
        private final int id;
        private final ClusterNode clusterNode;
        private final String nodeName;
        private boolean interrupted;
        private int tnxCommits;
        private int tnxAborts;

        public Worker(int id, ClusterNode clusterNode) {
            this.id = id;
            this.clusterNode = clusterNode;
            interrupted = false;
            nodeName = "worker_" + id;
            tnxCommits = 0;
            tnxAborts = 0;
            localStats = new DescriptiveStatistics();
        }

        public void setUp() {
            try (ContentSession session = clusterNode.newSession()) {
                Root rootTree = session.getLatestRoot();
                Tree parent = rootTree.getTree("/" + PARENT_NAME);
                parent.addChild(nodeName);
                rootTree.commit();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (CommitFailedException e) {
                e.printStackTrace();
            } catch (LoginException e) {
                e.printStackTrace();
            } catch (NoSuchWorkspaceException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            while (!interrupted) {
                long begin = System.currentTimeMillis();

                try (ContentSession session = clusterNode.newSession()) {

                    Root rootTree = session.getLatestRoot();
                    Tree node = rootTree.getTree("/"+PARENT_NAME+"/"+nodeName);

                    if (node.hasProperty(PROPERTY_NAME)) {
                        node.removeProperty(PROPERTY_NAME);
                    } else {
                        node.setProperty(PROPERTY_NAME, PROPERTY_VALUE);
                    }

                    rootTree.commit();
                    ++tnxCommits;

                } catch (CommitFailedException e) {
                    ++tnxAborts;
                } catch (Exception e) {
                    e.printStackTrace();
                }

                long runtime = System.currentTimeMillis() - begin;
                globalStats.addValue(runtime);
                localStats.addValue(runtime);
            }
        }


        public int getTnxCommits() {
            return tnxCommits;
        }

        public int getTnxAborts() {
            return tnxAborts;
        }

        @Override
        public String toString() {
            return "Worker " + id;
        }

        public DescriptiveStatistics getLocalStats() {
            return localStats;
        }
    }
}
