package org.k13n.oakconflicts;

import org.apache.jackrabbit.oak.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;

public class LocalConflictInIndexExample {
    private static final Logger logger = LoggerFactory.getLogger(LocalConflictInIndexExample.class);
    private static final String PROPERTY = "sessionId";
    private static final String DB_NAME = "test3";

    public static void main(String[] args) throws UnknownHostException {
        new LocalConflictInIndexExample().execute();
    }

    public void execute() {
        ClusterNode.dropDatabase(DB_NAME);
        setUpIndex();
        setUpExperiment();
        provokeConflicts();
    }

    private void setUpIndex() {
        ClusterNode cluster = newClusterNode();
        try (ContentSession session = cluster.newSession()) {
            Root rootTree = session.getLatestRoot();
            Tree root = rootTree.getTree("/");

            Tree index = root.addChild("oak:index");
            Tree sessionIndex = index.addChild("sessionId");
            sessionIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
            sessionIndex.setProperty("type", "property");
            sessionIndex.setProperty("propertyNames", PROPERTY);

            rootTree.commit();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (LoginException e) {
            e.printStackTrace();
        } catch (NoSuchWorkspaceException e) {
            e.printStackTrace();
        } catch (CommitFailedException e) {
            e.printStackTrace();
        } finally {
            cluster.tearDownRepository();
        }
    }

    private void setUpExperiment() {
        ClusterNode cluster = newClusterNode();
        try (ContentSession session = cluster.newSession()) {
            Root rootTree = session.getLatestRoot();
            Tree root = rootTree.getTree("/");

            Tree a = root.addChild("a");
            a.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            Tree b = a.addChild("b");
            b.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            b.setProperty(PROPERTY, 0);

            Tree c = a.addChild("c");
            c.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

            rootTree.commit();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (CommitFailedException e) {
            e.printStackTrace();
        } catch (LoginException e) {
            e.printStackTrace();
        } catch (NoSuchWorkspaceException e) {
            e.printStackTrace();
        } finally {
            cluster.tearDownRepository();
        }
    }

    private void provokeConflicts() {
        ClusterNode cluster = newClusterNode();

        Semaphore semaphore = new Semaphore(2);
        try {
            semaphore.acquire(2);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        Thread t1 = new Thread("session1") {
            @Override
            public void run() {
                try (ContentSession session1 = cluster.newSession()) {
                    Root rootTree1 = session1.getLatestRoot();
                    Tree b = rootTree1.getTree("/a/b");
                    b.removeProperty(PROPERTY);

                    semaphore.acquire();;
                    logger.info("committing session s1");
                    rootTree1.commit();
                    logger.info("session s1 committed");

                } catch (CommitFailedException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (UnknownHostException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                } catch (LoginException e1) {
                    e1.printStackTrace();
                } catch (NoSuchWorkspaceException e1) {
                    e1.printStackTrace();
                }
            }
        };

        Thread t2 = new Thread("session2") {
            @Override
            public void run() {
                try (ContentSession session2 = cluster.newSession()) {
                    Root rootTree2 = session2.getLatestRoot();
                    Tree c = rootTree2.getTree("/a/c");
                    c.setProperty(PROPERTY, 0);

                    semaphore.acquire();;
                    logger.info("committing session s2");
                    rootTree2.commit();
                    logger.info("session s2 committed");

                } catch (CommitFailedException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (UnknownHostException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                } catch (LoginException e1) {
                    e1.printStackTrace();
                } catch (NoSuchWorkspaceException e1) {
                    e1.printStackTrace();
                }
            }
        };

        t1.start();
        t2.start();

        semaphore.release(2);

        try {
            t1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.tearDownRepository();
    }

    private ClusterNode newClusterNode() {
        return new ClusterNode(DB_NAME, 1);
    }
}
