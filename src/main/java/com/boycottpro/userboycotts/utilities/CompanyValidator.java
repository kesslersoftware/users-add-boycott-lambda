package com.boycottpro.userboycotts.utilities;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.util.Map;

public class CompanyValidator {

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public CompanyValidator(DynamoDbClient dynamoDbClient, String tableName) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = tableName;
    }

    public boolean validateCompanyName(String company_id, String company_name) {
        try {
            GetItemRequest request = GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("company_id", AttributeValue.fromS(company_id)))
                    .attributesToGet("company_name")
                    .build();

            GetItemResponse response = dynamoDbClient.getItem(request);

            if (!response.hasItem()) {
                System.out.println("no company_id of that name exists");
                return false;
            }

            String storedName = response.item().get("company_name").s();
            return storedName.equals(company_name);

        } catch (DynamoDbException e) {
            // optionally log or rethrow
            return false;
        }
    }
}
