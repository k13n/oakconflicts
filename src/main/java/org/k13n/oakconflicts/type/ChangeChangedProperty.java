package org.k13n.oakconflicts.type;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeChangedProperty extends AbstractConflictTypeExample {
    private static final Logger logger = LoggerFactory.getLogger(ChangeDeletedProperty.class);

    @Override
    void setUpExperiment(Tree root) {
        root.addChild("a").setProperty("foo", "bar");
    }

    @Override
    void provokeConflicts(Root rootTree1, Root rootTree2) throws CommitFailedException {
        Tree n1 = rootTree1.getTree("/a");
        Tree n2 = rootTree2.getTree("/a");

        n1.setProperty("foo", "bar1");
        n2.setProperty("foo", "bar2");

        logger.info("committing session s1");
        rootTree1.commit();

        logger.info("committing session s2");
        rootTree2.commit();
    }

    public static void main(String[] args) {
        new ChangeChangedProperty().execute();
    }
}
