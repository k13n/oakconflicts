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
    private static final String PARENT_NAME = "workflows";
    private static final String PROPERTY_NAME = "status";
    private static final String PROPERTY_ON = "on";
    private static final String PROPERTY_OFF = "off";

    private final DescriptiveStatistics globalStats;
    private final ClusterNode[] cluster;
    private final Workflow[] workflows;
    private LinkedList<ClusterNode> freeClusterNodes;

    public Benchmark(int clusterSize, int nrWorkflows) {
        globalStats = new DescriptiveStatistics();
        cluster = new ClusterNode[clusterSize];
        workflows = new Workflow[nrWorkflows];
        freeClusterNodes = new LinkedList<>();
    }

    public void execute() {
        setUpExperiment();
        runExperiment();
        concludeExperiment();
        tearDownExperiment();
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

        for (int i = 0; i < workflows.length; ++i) {
            ClusterNode clusterNode;
            if (!freeClusterNodes.isEmpty()) {
                clusterNode = freeClusterNodes.removeFirst();
            } else {
                clusterNode = cluster[ThreadLocalRandom.current().nextInt(0, cluster.length)];
            }

            Workflow workflow = new Workflow(i, clusterNode);
            workflow.setUp();

            workflows[i] = workflow;
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

    private void tearDownExperiment() {
        for (int i = 0; i < cluster.length; ++i) {
            cluster[i].tearDownRepository();
        }
        ClusterNode.dropDatabase(DB_NAME);
    }


    private void runExperiment() {
        for (int i = 0; i < workflows.length; ++i) {
            workflows[i].start();
        }

        try {
            Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < workflows.length; ++i) {
            workflows[i].interrupted = true;
        }
        for (int i = 0; i < workflows.length; ++i) {
            try {
                workflows[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void concludeExperiment() {
        System.out.println("cluster size: " + cluster.length);
        System.out.println("nr workflows: " + workflows.length);
        System.out.println("N: " + globalStats.getN());
        System.out.println("min: " + globalStats.getMin());
        System.out.println("p10: " + globalStats.getPercentile(10.0));
        System.out.println("p50: " + globalStats.getPercentile(50.0));
        System.out.println("p90: " + globalStats.getPercentile(90.0));
        System.out.println("p99: " + globalStats.getPercentile(99.0));
        System.out.println("max: " + globalStats.getMax());
        // I had to patch Oak to get this function: Commit.conflictCounter.get()
        // So it won't compile on vanilla Oak
        System.out.println("#conflicts: " + Commit.conflictCounter.get());
        System.out.println();
        for (int i = 0; i < workflows.length; ++i) {
            Workflow workflow = workflows[i];
            System.out.println(workflow);
            System.out.println("commits: " + workflow.getTnxCommits());
            System.out.println("aborts:  " + workflow.getTnxAborts());
            System.out.println("avgRuntime: " + workflow.getLocalStats().getMean());
        }
    }

    public class Workflow extends Thread {
        private final DescriptiveStatistics localStats;
        private final int id;
        private final ClusterNode clusterNode;
        private final String nodeName;
        private boolean interrupted;
        private int tnxCommits;
        private int tnxAborts;

        public Workflow(int id, ClusterNode clusterNode) {
            this.id = id;
            this.clusterNode = clusterNode;
            interrupted = false;
            nodeName = "Workflow" + id;
            tnxCommits = 0;
            tnxAborts = 0;
            localStats = new DescriptiveStatistics();
        }

        public void setUp() {
            try (ContentSession session = clusterNode.newSession()) {
                Root rootTree = session.getLatestRoot();
                Tree parent = rootTree.getTree("/" + PARENT_NAME);
                Tree child  = parent.addChild(nodeName);

                if (Math.random() < 0.5) {
                    child.setProperty(PROPERTY_NAME, PROPERTY_ON);
                } else {
                    child.setProperty(PROPERTY_NAME, PROPERTY_OFF);
                }

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

                    if (PROPERTY_ON.equals(node.getProperty(PROPERTY_NAME).getValue(Type.STRING))) {
                        node.setProperty(PROPERTY_NAME, PROPERTY_OFF);
                    } else {
                        node.setProperty(PROPERTY_NAME, PROPERTY_ON);
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
            return "Workflow " + id;
        }

        public DescriptiveStatistics getLocalStats() {
            return localStats;
        }
    }
}
