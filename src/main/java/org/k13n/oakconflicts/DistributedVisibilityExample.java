package org.k13n.oakconflicts;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.UnknownHostException;

public class DistributedVisibilityExample {
    private static final Logger logger = LoggerFactory.getLogger(DistributedVisibilityExample.class);
    private static final int ASYNC_DELAY_MS = 5000;
    private static final String DB_NAME = "test6";

    public static void main(String[] args) throws UnknownHostException {
        new DistributedVisibilityExample().execute();
    }

    public void execute() throws UnknownHostException {
        setUpExperiment();
        provokeConflicts();
    }

    private void setUpExperiment() {
        ClusterNode.dropDatabase(DB_NAME);
        ClusterNode cluster = newClusterNode(1);
        cluster.tearDownRepository();
    }

    private void provokeConflicts() {
        ClusterNode cluster1 = newClusterNode(2);
        ClusterNode cluster2 = newClusterNode(3);

        try (ContentSession session1 = cluster1.newSession();
             ContentSession session2 = cluster2.newSession()) {

            Root rootTree1 = session1.getLatestRoot();
            Root rootTree2 = session2.getLatestRoot();

            Tree root1 = rootTree1.getTree("/");
            Tree root2 = rootTree2.getTree("/");


            // create some data and commit

            root1.addChild("a");
            root2.addChild("b");

            logger.info("committing session s1");
            rootTree1.commit();

            logger.info("committing session s2");
            rootTree2.commit();


            // check if session1 sees changes made by session2

            rootTree1.refresh();
            logger.info("/b visible: " + root1.getChild("b").exists());


            Thread.sleep(2 * ASYNC_DELAY_MS);

            rootTree1.refresh();
            logger.info("/b visible: " + root1.getChild("b").exists());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (CommitFailedException e) {
            e.printStackTrace();
        } catch (LoginException e) {
            e.printStackTrace();
        } catch (NoSuchWorkspaceException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            cluster1.tearDownRepository();
            cluster2.tearDownRepository();
        }
    }

    private ClusterNode newClusterNode(int id) {
        return new ClusterNode(DB_NAME, id, ASYNC_DELAY_MS);
    }
}
