package org.k13n.oakconflicts.type;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteDeletedNode extends AbstractConflictTypeExample {
    private static final Logger logger = LoggerFactory.getLogger(DeleteDeletedNode.class);

    @Override
    void setUpExperiment(Tree root) {
        Tree a = root.addChild("a");
        a.addChild("b");
    }

    @Override
    void provokeConflicts(Root rootTree1, Root rootTree2) throws CommitFailedException {
        Tree n1 = rootTree1.getTree("/a/b");
        Tree n2 = rootTree2.getTree("/a/b");

        n1.remove();
        n2.remove();

        logger.info("committing session s1");
        rootTree1.commit();

        logger.info("committing session s2");
        rootTree2.commit();
    }

    public static void main(String[] args) {
        new DeleteDeletedNode().execute();
    }
}
