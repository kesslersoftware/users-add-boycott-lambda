package com.boycottpro.userboycotts;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.boycottpro.models.ResponseMessage;
import com.boycottpro.userboycotts.model.AddBoycottForm;
import com.boycottpro.utilities.CauseValidator;
import com.boycottpro.utilities.CompanyValidator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.*;

public class AddUserBoycottsHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final DynamoDbClient dynamoDb;
    private final ObjectMapper objectMapper = new ObjectMapper();
    public AddUserBoycottsHandler() {
        this.dynamoDb = DynamoDbClient.create();
    }

    public AddUserBoycottsHandler(DynamoDbClient dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        try {
            AddBoycottForm input = objectMapper.readValue(event.getBody(), AddBoycottForm.class);
            System.out.println("AddBoycottForm = " + input);
            String userId = input.getUser_id();
            String companyId = input.getCompany_id();
            String companyName = input.getCompany_name();
            List<AddBoycottForm.Reason> reasons = input.getReasons();
            String personalReason = input.getPersonal_reason();
            if (userId == null || companyId == null ||
                    ((reasons == null || reasons.isEmpty()) && (personalReason == null || personalReason.isBlank()))) {
                return response(400, "Either reasons or personal_reason must be provided.");
            }
            // validate company_id
            CompanyValidator companyValidator = new CompanyValidator(this.dynamoDb,"companies");
            boolean validCompany = companyValidator.validateCompanyName(companyId,companyName);
            if(!validCompany) {
                System.out.println("company_name do not match!");
                throw new RuntimeException("not a valid company!");
            }
            String now = Instant.now().toString();
            boolean userHasBoycott = userHasAnyBoycott(userId, companyId);
            System.out.println("user has boycott = " + userHasBoycott);
            boolean anySuccess = false;
            List<String> errors = new ArrayList<>();

            // B: Reason-based boycotts
            for (AddBoycottForm.Reason reason : reasons) {
                String causeId = reason.getCause_id();
                System.out.println("reason = " + causeId);
                if (userHasSpecificBoycott(userId, companyId, causeId)) {
                    System.out.println("user has a specific boycott for cause ID = " + causeId);
                    continue;
                }
                CauseValidator causeValidator = new CauseValidator(this.dynamoDb,"causes");
                boolean validCause = causeValidator.validateCauseDescription(causeId, reason.getCause_desc());
                if(!validCause) {
                    System.out.println("cause ID = " + causeId + " is not valid!");
                    continue;
                }
                List<TransactWriteItem> actions = new ArrayList<>();

                // user_boycotts record
                actions.add(TransactWriteItem.builder()
                        .put(Put.builder().tableName("user_boycotts")
                                .item(Map.of(
                                        "user_id", AttributeValue.fromS(userId),
                                        "company_id", AttributeValue.fromS(companyId),
                                        "company_name", AttributeValue.fromS(companyName),
                                        "cause_id", AttributeValue.fromS(causeId),
                                        "cause_desc", AttributeValue.fromS(reason.getCause_desc()),
                                        "company_cause_id", AttributeValue.fromS(companyId+"#"+causeId),
                                        "timestamp", AttributeValue.fromS(now)
                                )).build()).build());
                System.out.println("user_boycotts added");
                // user_causes record if not already following
                if (!userIsFollowingCause(userId, causeId)) {
                    System.out.println("user is not following this cause");
                    actions.add(TransactWriteItem.builder()
                            .put(Put.builder().tableName("user_causes")
                                    .item(Map.of(
                                            "user_id", AttributeValue.fromS(userId),
                                            "cause_id", AttributeValue.fromS(causeId),
                                            "cause_desc", AttributeValue.fromS(reason.getCause_desc()),
                                            "timestamp", AttributeValue.fromS(now)
                                    )).build()).build());
                    System.out.println("user_causes added");
                    // causes update
                    actions.add(TransactWriteItem.builder()
                            .update(Update.builder()
                                    .tableName("causes")
                                    .key(Map.of("cause_id", AttributeValue.fromS(causeId)))
                                    .updateExpression("SET follower_count = if_not_exists(follower_count, :zero) + :inc")
                                    .expressionAttributeValues(Map.of(
                                            ":zero", AttributeValue.fromN("0"),
                                            ":inc", AttributeValue.fromN("1")
                                    ))
                                    .build()).build());
                    System.out.println("causes added");
                }
                // cause_company_stats update
                if (findCauseCompanyRecord(causeId, companyId)) {
                    // update existing record
                    actions.add(
                            TransactWriteItem.builder()
                                    .update(Update.builder()
                                            .tableName("cause_company_stats")
                                            .key(Map.of(
                                                    "cause_id", AttributeValue.fromS(causeId),
                                                    "company_id", AttributeValue.fromS(companyId)
                                            ))
                                            .updateExpression("SET boycott_count = if_not_exists(boycott_count, :zero) + :inc")
                                            .expressionAttributeValues(Map.of(
                                                    ":zero", AttributeValue.fromN("0"),
                                                    ":inc", AttributeValue.fromN("1")
                                            ))
                                            .build())
                                    .build()
                    );
                } else {
                    // insert new record
                    actions.add(
                            TransactWriteItem.builder()
                                    .put(Put.builder()
                                            .tableName("cause_company_stats")
                                            .item(Map.of(
                                                    "cause_id", AttributeValue.fromS(causeId),
                                                    "company_id", AttributeValue.fromS(companyId),
                                                    "company_name", AttributeValue.fromS(companyName),
                                                    "cause_desc", AttributeValue.fromS(reason.getCause_desc()),
                                                    "boycott_count", AttributeValue.fromN("1")
                                            ))
                                            .build())
                                    .build()
                    );
                }
                System.out.println("cause_company_stats added");
                try {
                    // Execute this transaction
                    System.out.println("Execute this transaction");
                    for(TransactWriteItem action : actions) {
                        System.out.println(action.toString());
                    }
                    dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder()
                            .transactItems(actions).build());
                    anySuccess = true;
                    System.out.println("transaction successful for cause = " + causeId);
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    errors.add("Failed to record boycott for cause: " + causeId + " -> " + e.getMessage());
                }
            }
            System.out.println("checking personalReason = " + personalReason);
            // C: Personal reason
            if (personalReason != null && !personalReason.isBlank()
                    && !userHasPersonalReason(userId, companyId, personalReason)) {
                System.out.println("user does not have this personal reason");
                List<TransactWriteItem> actions = List.of(
                        TransactWriteItem.builder()
                                .put(Put.builder()
                                        .tableName("user_boycotts")
                                        .item(Map.of(
                                                "user_id", AttributeValue.fromS(userId),
                                                "company_id", AttributeValue.fromS(companyId),
                                                "company_name", AttributeValue.fromS(companyName),
                                                "company_cause_id", AttributeValue.fromS(personalReason+"#"+companyId),
                                                "timestamp", AttributeValue.fromS(now),
                                                "personal_reason", AttributeValue.fromS(personalReason)
                                        )).build()).build()
                );
                System.out.println("user_boycotts added");
                try {
                    // Execute this transaction
                    System.out.println("Execute this transaction for personal reasons");
                    dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder()
                            .transactItems(actions).build());
                    anySuccess = true;
                    System.out.println("transaction successful for personal reason = " + personalReason);
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    errors.add("Failed to record boycott for personal reason: " + personalReason + " -> " + e.getMessage());
                }
            }

            // D: Company update
            if (!userHasBoycott && anySuccess) {
                System.out.println("going to increment companies record");
                dynamoDb.updateItem(UpdateItemRequest.builder()
                        .tableName("companies")
                        .key(Map.of("company_id", AttributeValue.fromS(companyId)))
                        .updateExpression("SET boycott_count = boycott_count + :inc")
                        .expressionAttributeValues(Map.of(
                                ":inc", AttributeValue.fromN("1")
                        )).build());
            }

            if (!anySuccess) {
                System.out.println("returning a 409 code");
                return response(409, "No new boycotts were recorded. Possible duplicates.");
            } else if (!errors.isEmpty()) {
                System.out.println("returning a 207 code");
                return response(207, "Some boycotts recorded. Errors: " + objectMapper.writeValueAsString(errors));
            } else {
                return response(200, "All boycotts recorded successfully.");
            }

        } catch (Exception e) {
            e.printStackTrace();
            return response(500, "Transaction failed: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent response(int status, String body)  {
        ResponseMessage message = new ResponseMessage(status,body,
                body);
        String responseBody = null;
        try {
            responseBody = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(status)
                .withHeaders(Map.of("Content-Type", "application/json"))
                .withBody(responseBody);
    }

    public boolean findCauseCompanyRecord(String causeId, String companyId) {
        try {
            GetItemRequest request = GetItemRequest.builder()
                    .tableName("cause_company_stats")
                    .key(Map.of(
                            "cause_id", AttributeValue.fromS(causeId),
                            "company_id", AttributeValue.fromS(companyId)
                    ))
                    .build();

            GetItemResponse response = dynamoDb.getItem(request);
            return response.hasItem();

        } catch (DynamoDbException e) {
            // Optional: log the exception or rethrow
            return false;
        }
    }

    private boolean userHasAnyBoycott(String userId, String companyId) {
        try {
            QueryRequest request = QueryRequest.builder()
                    .tableName("user_boycotts")
                    .keyConditionExpression("user_id = :uid")
                    .expressionAttributeValues(Map.of(
                            ":uid", AttributeValue.builder().s(userId).build()
                    ))
                    .build();

            QueryResponse response = dynamoDb.query(request);

            return response.items().stream()
                    .anyMatch(item -> companyId.equals(item.get("company_id").s()));

        } catch (DynamoDbException e) {
            System.err.println("DynamoDB query failed: " + e.getMessage());
            return false;
        }
    }

    private boolean userHasSpecificBoycott(String userId, String companyId, String causeId) {
        System.out.println("checking for specific boycotts for this company");
        QueryRequest request = QueryRequest.builder()
                .tableName("user_boycotts")
                .keyConditionExpression("user_id = :uid AND company_cause_id = :caid")
                .expressionAttributeValues(Map.of(
                        ":uid", AttributeValue.builder().s(userId).build(),
                        ":caid", AttributeValue.builder().s(companyId+"#"+causeId).build()
                ))
                .limit(1)
                .build();
        System.out.println("checked for specific boycotts for this company");
        return !dynamoDb.query(request).items().isEmpty();
    }

    public boolean userHasPersonalReason(String userId, String companyId, String personalReason) {
        try {
            QueryRequest queryRequest = QueryRequest.builder()
                    .tableName("user_boycotts")
                    .keyConditionExpression("user_id = :uid")
                    .expressionAttributeValues(Map.of(":uid", AttributeValue.fromS(userId)))
                    .build();

            QueryResponse response = dynamoDb.query(queryRequest);
            boolean alreadyHasPersonalReason = response.items().stream().anyMatch(item ->
                    item.containsKey("company_id") &&
                            item.get("company_id").s().equals(companyId) &&
                            item.containsKey("personal_reason") &&
                            !item.get("personal_reason").s().isBlank() &&
                            item.get("personal_reason").s().equalsIgnoreCase(personalReason)
            );
            System.out.println("alreadyHasPersonalReason = " + alreadyHasPersonalReason);
            return alreadyHasPersonalReason;

        } catch (DynamoDbException e) {
            System.out.println("issue checking the personal reason");
            e.printStackTrace();
            return false;
        }
    }


    private boolean userIsFollowingCause(String userId, String causeId) {
        QueryRequest request = QueryRequest.builder()
                .tableName("user_causes")
                .keyConditionExpression("user_id = :uid AND cause_id = :cid")
                .expressionAttributeValues(Map.of(
                        ":uid", AttributeValue.builder().s(userId).build(),
                        ":cid", AttributeValue.builder().s(causeId).build()
                ))
                .limit(1)
                .build();

        return !dynamoDb.query(request).items().isEmpty();
    }
}