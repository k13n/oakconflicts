package org.k13n.oakconflicts.type;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.k13n.oakconflicts.ClusterNode;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.io.IOException;

public abstract class AbstractConflictTypeExample {
    private static final String DB_NAME = "test";

    public void execute() {
        setUpExperiment();
        provokeConflicts();
    }

    private ClusterNode newClusterNode() {
        return new ClusterNode(DB_NAME, 1);
    }

    public void setUpExperiment() {
        ClusterNode.dropDatabase(DB_NAME);
        ClusterNode cluster = newClusterNode();
        try (ContentSession session = cluster.newSession()) {
            Root rootTree = session.getLatestRoot();
            Tree root = rootTree.getTree("/");
            setUpExperiment(root);
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

    public void provokeConflicts() {
        ClusterNode cluster = newClusterNode();

        try (ContentSession session1 = cluster.newSession();
             ContentSession session2 = cluster.newSession()) {

            Root rootTree1 = session1.getLatestRoot();
            Root rootTree2 = session2.getLatestRoot();

            provokeConflicts(rootTree1, rootTree2);

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

    abstract void setUpExperiment(Tree root);
    abstract void provokeConflicts(Root rootTree1, Root rootTree2)
            throws CommitFailedException;
}
