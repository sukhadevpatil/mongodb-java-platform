package com.mongodb.javaplatform.mongodbjavaplatform;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

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

}
