package org.k13n.oakconflicts;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;

import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;
import java.net.UnknownHostException;

public class ClusterNode {
    private final int id;
    private DocumentNodeStore nodeStore;
    private ContentRepository repository;

    public ClusterNode(String dbName, int clusterId) {
        this(dbName, clusterId, 1000);
    }

    public ClusterNode(String dbName, int clusterId, int asyncDelay) {
        id = clusterId;
        try {
            DB db = new MongoClient("127.0.0.1", 27017).getDB(dbName);
            nodeStore = new DocumentMK.Builder().
                    setMongoDB(db).
                    setAsyncDelay(asyncDelay).
                    setClusterId(clusterId).
                    getNodeStore();
            repository = new Oak(nodeStore).
                    with(new InitialContent()).
                    with(new OpenSecurityProvider()).
                    with(JcrConflictHandler.createJcrConflictHandler()).
                    with(new ConflictValidatorProvider()).
                    with(new PropertyIndexEditorProvider()).
                    with(new PropertyIndexProvider()).
                    createContentRepository();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void dropDatabase(String dbName) {
        try {
            MongoClient client = new MongoClient();
            client.getDB(dbName).dropDatabase();
            client.close();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void tearDownRepository() {
        nodeStore.dispose();
    }

    public ContentSession newSession() throws UnknownHostException, NoSuchWorkspaceException, LoginException {
        Credentials credentials = new SimpleCredentials("admin", "admin".toCharArray());
        return repository.login(credentials, "default");
    }
}
