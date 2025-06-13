package com.boycottpro.userboycotts.utilities;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.util.Map;

public class CauseValidator {

    private final DynamoDbClient dynamoDbClient;
    private final String tableName;

    public CauseValidator(DynamoDbClient dynamoDbClient, String tableName) {
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = tableName;
    }

    public boolean validateCauseDescription(String cause_id, String cause_desc) {
        try {
            GetItemRequest request = GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("cause_id", AttributeValue.fromS(cause_id)))
                    .attributesToGet("cause_desc")
                    .build();

            GetItemResponse response = dynamoDbClient.getItem(request);

            if (!response.hasItem()) {
                return false;
            }

            String storedDesc = response.item().get("cause_desc").s();
            return storedDesc.equals(cause_desc);

        } catch (DynamoDbException e) {
            // optionally log or rethrow
            return false;
        }
    }
}

