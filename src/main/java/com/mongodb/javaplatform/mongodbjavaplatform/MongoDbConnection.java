package com.mongodb.javaplatform.mongodbjavaplatform;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoDbConnection {

    public static void main(String[] args) {
        MongoClient mongoClient = connection();
        System.out.println("==============");
        mongoClient.listDatabaseNames().forEach(val -> System.out.println(val));
        System.out.println("=============");
    }

    static MongoClient connection() {
        MongoClient mongoClient = null;
        try {
            ConnectionString connectionString = new ConnectionString("mongodb://localhost:27017");

            MongoClientSettings clientSettings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .serverApi(ServerApi.builder().version(ServerApiVersion.V1).build())
                    .build();

            mongoClient = MongoClients.create(clientSettings);
        } catch (Exception exc) {
            System.out.println(exc);
        }


        return mongoClient;
    }
}
