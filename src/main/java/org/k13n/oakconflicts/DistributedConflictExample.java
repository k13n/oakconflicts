package org.k13n.oakconflicts;

import org.apache.jackrabbit.oak.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.UnknownHostException;

public class DistributedConflictExample {
    private static final Logger logger = LoggerFactory.getLogger(DistributedConflictExample.class);
    private static final int ASYNC_DELAY_MS = 10000;

    public static void main(String[] args) throws UnknownHostException {
        new DistributedConflictExample().execute();
    }

    public void execute() throws UnknownHostException {
        setUpExperiment();
        provokeConflicts();
    }

    private void setUpExperiment() {
        ClusterNode cluster = newClusterNode(1);
        try (ContentSession session = cluster.newSession()) {
            Root rootTree = session.getLatestRoot();
            Tree root = rootTree.getTree("/");

            Tree a = root.addChild("a");
            a.setProperty("sessionId", 0);

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
        ClusterNode cluster1 = newClusterNode(2);
        ClusterNode cluster2 = newClusterNode(3);

        try (ContentSession session1 = cluster1.newSession();
             ContentSession session2 = cluster2.newSession()) {

            Root rootTree1 = session1.getLatestRoot();
            Root rootTree2 = session2.getLatestRoot();

            Tree a1 = rootTree1.getTree("/a");
            Tree a2 = rootTree2.getTree("/a");

            a1.setProperty("sessionId", 1);
            a2.setProperty("sessionId", 2);

            logger.info("committing session s1");
            rootTree1.commit();

            logger.info("committing session s2");
            rootTree2.commit();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (CommitFailedException e) {
            e.printStackTrace();
        } catch (LoginException e) {
            e.printStackTrace();
        } catch (NoSuchWorkspaceException e) {
            e.printStackTrace();
        } finally {
            cluster1.tearDownRepository();
            cluster2.tearDownRepository();
        }
    }

    private ClusterNode newClusterNode(int id) {
        return new ClusterNode("test2", id, ASYNC_DELAY_MS);
    }
}
