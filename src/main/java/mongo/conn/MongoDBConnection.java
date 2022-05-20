package mongo.conn;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;

@Getter
public class MongoDBConnection {

        private MongoDatabase mongodb;
        private MongoClient mongoClient;

        public MongoDBConnection() {

                String hostName = "192.168.52.129";
                int port = 27017;
                String userName = "myUser";
                String password = "1234";
                String db = "myDB";

                mongoClient = new MongoClient(hostName, port);

                MongoCredential.createCredential(userName, db, password.toCharArray());

                mongodb = mongoClient.getDatabase(db);
        }
}
