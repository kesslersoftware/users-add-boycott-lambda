package com.boycottpro.userboycotts;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.boycottpro.userboycotts.model.AddBoycottForm;
import com.boycottpro.userboycotts.utilities.CauseValidator;
import com.boycottpro.userboycotts.utilities.CompanyValidator;
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
            CompanyValidator companyValidator = new CompanyValidator(this.dynamoDb,"user_boycotts");
            boolean validCompany = companyValidator.validateCompanyName(companyId,companyName);
            if(!validCompany) {
                System.out.println("comapny_name do not match!");
                throw new RuntimeException("not a valid company!");
            }
            String now = Instant.now().toString();
            boolean userHasBoycott = userHasAnyBoycott(userId, companyId);
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
                actions.add(TransactWriteItem.builder()
                        .update(Update.builder()
                                .tableName("cause_company_stats")
                                .key(Map.of(
                                        "cause_id", AttributeValue.fromS(causeId),
                                        "cause_desc", AttributeValue.fromS(reason.getCause_desc()),
                                        "company_id", AttributeValue.fromS(companyId),
                                        "company_name", AttributeValue.fromS(companyName)
                                ))
                                .updateExpression("SET boycott_count = if_not_exists(boycott_count, :zero) + :inc")
                                .expressionAttributeValues(Map.of(
                                        ":zero", AttributeValue.fromN("0"),
                                        ":inc", AttributeValue.fromN("1")
                                ))
                                .build()).build());
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
                    errors.add("Failed to record boycott for cause: " + causeId + " -> " + e.getMessage());
                }
            }

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
                    errors.add("Failed to record boycott for personal reason: " + personalReason + " -> " + e.getMessage());
                }
            }

            // D: Company update
            if (!userHasBoycott && anySuccess) {
                System.out.println("going to increment companies record");
                dynamoDb.updateItem(UpdateItemRequest.builder()
                        .tableName("companies")
                        .key(Map.of("company_id", AttributeValue.fromS(companyId)))
                        .updateExpression("SET boycott_count = if_not_exists(boycott_count, :zero) + :inc")
                        .expressionAttributeValues(Map.of(
                                ":zero", AttributeValue.fromN("0"),
                                ":inc", AttributeValue.fromN("1")
                        )).build());
            }

            if (!anySuccess) {
                return response(409, "No new boycotts were recorded. Possible duplicates.");
            } else if (!errors.isEmpty()) {
                return response(207, "Some boycotts recorded. Errors: " + objectMapper.writeValueAsString(errors));
            } else {
                return response(200, "All boycotts recorded successfully.");
            }

        } catch (Exception e) {
            return response(500, "Transaction failed: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent response(int status, String body) {
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(status)
                .withHeaders(Map.of("Content-Type", "application/json"))
                .withBody(body);
    }

    private boolean userHasAnyBoycott(String userId, String companyId) {
        System.out.println("checking for previous boycotts for this company");
        QueryRequest request = QueryRequest.builder()
                .tableName("user_boycotts")
                .keyConditionExpression("user_id = :uid AND company_id = :cid")
                .expressionAttributeValues(Map.of(
                        ":uid", AttributeValue.builder().s(userId).build(),
                        ":cid", AttributeValue.builder().s(companyId).build()
                ))
                .limit(1)
                .build();
        System.out.println("checked for previous boycotts for this company");
        return !dynamoDb.query(request).items().isEmpty();
    }

    private boolean userHasSpecificBoycott(String userId, String companyId, String causeId) {
        System.out.println("checking for specific boycotts for this company");
        QueryRequest request = QueryRequest.builder()
                .tableName("user_boycotts")
                .keyConditionExpression("user_id = :uid AND company_id = :cid")
                .filterExpression("cause_id = :caid")
                .expressionAttributeValues(Map.of(
                        ":uid", AttributeValue.builder().s(userId).build(),
                        ":cid", AttributeValue.builder().s(companyId).build(),
                        ":caid", AttributeValue.builder().s(causeId).build()
                ))
                .limit(1)
                .build();
        System.out.println("checked for specific boycotts for this company");
        return !dynamoDb.query(request).items().isEmpty();
    }

    private boolean userHasPersonalReason(String userId, String companyId, String personalReason) {
        // this method is checking to see if the user already has the SAME personal reason
        System.out.println("checking for personal reason for this company");
        QueryRequest request = QueryRequest.builder()
                .tableName("user_boycotts")
                .keyConditionExpression("user_id = :uid AND company_id = :cid")
                .filterExpression("personal_reason = :reason")
                .expressionAttributeValues(Map.of(
                        ":uid", AttributeValue.builder().s(userId).build(),
                        ":cid", AttributeValue.builder().s(companyId).build(),
                        ":reason", AttributeValue.builder().s(personalReason).build()
                ))
                .limit(1)
                .build();
        System.out.println("checked for personal reason for this company");
        return !dynamoDb.query(request).items().isEmpty();
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