package org.k13n.oakconflicts;

import org.apache.jackrabbit.oak.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.UnknownHostException;

public class LocalRebaseInIndexExample {
    private static final Logger logger = LoggerFactory.getLogger(LocalRebaseInIndexExample.class);
    private static final String PROPERTY = "sessionId";
    private static final String DB_NAME = "test5";

    public static void main(String[] args) throws UnknownHostException {
        new LocalRebaseInIndexExample().execute();
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

        try (ContentSession session1 = cluster.newSession();
             ContentSession session2 = cluster.newSession()) {

            Root rootTree1 = session1.getLatestRoot();
            Root rootTree2 = session2.getLatestRoot();

            Tree b = rootTree1.getTree("/a/b");
            Tree c = rootTree2.getTree("/a/c");

            b.removeProperty(PROPERTY);
            c.setProperty(PROPERTY, 0);

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
        return new ClusterNode(DB_NAME, 1);
    }
}
