package com.mongodb.javaplatform.mongodbjavaplatform;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoDbConnection {

    private MongoDbConnection() {
    }

    public static MongoClient INSTANCE = null;

    public static MongoClient getInstance() {
        if (INSTANCE == null) {
            try {
                ConnectionString connectionString = new ConnectionString("mongodb://localhost:27017");

                MongoClientSettings clientSettings = MongoClientSettings
                        .builder()
                        .applyConnectionString(connectionString)
                        .serverApi(ServerApi.builder().version(ServerApiVersion.V1).build())
                        .build();

                INSTANCE = MongoClients.create(clientSettings);
            } catch (Exception exc) {
                System.out.println(exc);
            }
        }

        return INSTANCE;
    }

}
