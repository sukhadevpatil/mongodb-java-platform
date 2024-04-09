package com.mongodb.javaplatform.mongodbjavaplatform;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Filters.*;
import static java.lang.Long.sum;

@SpringBootTest
class MongodbJavaPlatformApplicationTests {

    @Test
        //@Disabled
    void contextLoads() {
    }

    @Test
    void testConnection() {
        MongoClient client = MongoDbConnection.getInstance();

        System.out.println("==============");
        client.listDatabaseNames().forEach(System.out::println);
        System.out.println("=============");

        Assertions.assertNotNull(client.listDatabaseNames());
    }

    @Test
    void testDatabases() {
        MongoClient client = MongoDbConnection.getInstance();
        List<Document> databases = client.listDatabases().into(new ArrayList<>());

        System.out.println("==============");
        databases.forEach(db -> System.out.println(db.toJson()));
        System.out.println("==============");

        Assertions.assertNotNull(databases);
    }

    @Test
    void insertOneRecord() {
        MongoClient client = MongoDbConnection.getInstance();

        MongoDatabase database = client.getDatabase("training");
        MongoCollection<Document> collection = database.getCollection("inspections");

        Document document = new Document("_id", new ObjectId()).append("id", "10021-2015-ENFO").append("certificate_number", 9278806).append("business_name", "ATLIXCO DELI GROCERY INC.").append("date", Date.from(LocalDate.of(2015, 2, 20).atStartOfDay(ZoneId.systemDefault()).toInstant())).append("result", "No Violation Issued").append("sector", "Cigarette Retail Dealer - 127").append("address", new Document().append("city", "RIDGEWOOD").append("zip", 11385).append("street", "MENAHAN ST").append("number", 1712));

        InsertOneResult result = collection.insertOne(document);
        BsonValue id = result.getInsertedId();
        System.out.println(id);

        Assertions.assertNotNull(id);
    }

    @Test
    void insertMultipleRecords() {
        MongoClient client = MongoDbConnection.getInstance();

        MongoDatabase database = client.getDatabase("bank");
        MongoCollection<Document> collection = database.getCollection("accounts");

        Document doc1 = new Document().append("account_holder", "john doe").append("account_id", "MDB99115881").append("balance", 1785).append("account_type", "checking");
        Document doc2 = new Document().append("account_holder", "jane doe").append("account_id", "MDB79101843").append("balance", 1468).append("account_type", "checking");
        List<Document> accounts = Arrays.asList(doc1, doc2);

        InsertManyResult result = collection.insertMany(accounts);
        Map<Integer, BsonValue> mapIds = result.getInsertedIds();

        System.out.println("==================");
        mapIds.forEach((k, v) -> {
            System.out.println(k + " :: val :: " + v);
        });
        System.out.println("==================");

        Assertions.assertNotNull(mapIds);
    }

    @Test
    void testQueryingCollection() {
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("books");

        List<Document> documents = collection.find(and(eq("status", "PUBLISH"), gte("pageCount", 400), in("categories", "Java"))).into(new ArrayList<>());

        documents.forEach(doc -> System.out.println(doc.toJson()));

        Assertions.assertNotNull(documents);
    }

    @Test
    void testQueryingFindFirstCollection() {
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("books");

        Document document = collection.find(and(eq("status", "PUBLISH"), gte("pageCount", 400), in("categories", "Java"))).first();

        assert document != null;
        System.out.println(document.toJson());

        Assertions.assertNotNull(document.toJson());
    }

    @Test
    void updateOneRecord() {
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("books");

        Bson query = Filters.eq("isbn", "1933988673");

        Bson updates = Updates.combine(Updates.set("account_status", "active"), Updates.inc("pageCount", 1));
        UpdateResult upResult = collection.updateOne(query, updates);

        System.out.println("===========");
        System.out.println(upResult);
        System.out.println("===========");

        Assertions.assertNotNull(upResult);
    }


    @Test
    void updateManyRecord() {
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("books");

        Bson query = Filters.eq("status", "PUBLISH");
        Bson updates = Updates.combine(Updates.set("account_status", "active"), Updates.inc("pageCount", 1));
        UpdateResult upResult = collection.updateMany(query, updates);

        System.out.println("===========");
        System.out.println(upResult);
        System.out.println("===========");

        Assertions.assertNotNull(upResult);
    }

    @Test
    void deleteOneRecord() {
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("books");

        Bson query = Filters.lt("pageCount", 100);

        DeleteResult deleteResult = collection.deleteOne(query);

        System.out.println("===========");
        System.out.println(deleteResult);
        System.out.println("===========");
        System.out.println(deleteResult.getDeletedCount());
        System.out.println("===========");

        Assertions.assertNotNull(deleteResult);
    }

    @Test
    void deleteManyRecord() {
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("books");

        Bson query = Filters.lt("pageCount", 100);

        DeleteResult deleteResult = collection.deleteMany(query);

        System.out.println("===========");
        System.out.println(deleteResult);
        System.out.println("===========");
        System.out.println(deleteResult.getDeletedCount());
        System.out.println("===========");

        Assertions.assertNotNull(deleteResult);
    }

    @Test
    void queryingOnArrayElements() {
        //$elemMatch operator matches more than one component within an array element
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("grades");

        List<Document> documents = collection.find(elemMatch("scores", eq("type", "exam"))).into(new ArrayList<>());

        System.out.println("=================");
        documents.forEach(doc -> System.out.println(doc.toJson()));
        System.out.println("=================");

        Assertions.assertNotNull(documents);
    }

    @Test
    void findByAndOrOperators() {
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("grades");

        List<Document> documents = collection.find(and(eq("scores.type", "quiz"), gte("scores.score", 80))).into(new ArrayList<>());

        System.out.println("=================");
        documents.forEach(doc -> System.out.println(doc.toJson()));
        System.out.println("=================");

        Assertions.assertNotNull(documents);
    }

    @Test
    void aggregationMatchGroup() {
        MongoDatabase database = MongoDbConnection.getInstance().getDatabase("sample_data");

        MongoCollection<Document> collection = database.getCollection("books");

        Bson matchStage = Aggregates.match(Filters.eq("categories", "Java"));
        Bson groupStage = Aggregates.group("$account_type", avg("pageCountUpdated", "$pageCount"));
        System.out.println("Display aggregation results");
        List<Document> documents = collection.aggregate(List.of(matchStage, groupStage)).into(new ArrayList<>());

        System.out.println("=================");
        documents.forEach(document -> System.out.print(document.toJson()));
        System.out.println("=================");

        Assertions.assertNotNull(documents);
    }


}