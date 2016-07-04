package org.k13n.oakconflicts;

import org.apache.jackrabbit.oak.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.UnknownHostException;

public class LocalConflictExample {
    private static final Logger logger = LoggerFactory.getLogger(LocalConflictExample.class);

    public static void main(String[] args) throws UnknownHostException {
        new LocalConflictExample().execute();
    }

    public void execute() {
        setUpExperiment();
        provokeConflicts();
    }

    private void setUpExperiment() {
        ClusterNode cluster = newClusterNode();
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
        ClusterNode cluster = newClusterNode();

        try (ContentSession session1 = cluster.newSession();
             ContentSession session2 = cluster.newSession()) {

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
            cluster.tearDownRepository();
        }
    }

    private ClusterNode newClusterNode() {
        return new ClusterNode("test1", 1);
    }
}
