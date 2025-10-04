package com.boycottpro.userboycotts;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.boycottpro.models.ResponseMessage;
import com.boycottpro.userboycotts.model.AddBoycottForm;
import com.boycottpro.utilities.CauseValidator;
import com.boycottpro.utilities.CompanyValidator;
import com.boycottpro.utilities.JwtUtility;
import com.boycottpro.utilities.Logger;
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
        String sub = null;
        int lineNum = 37;
        try {
            sub = JwtUtility.getSubFromRestEvent(event);
            if (sub == null) {
            Logger.error(41, sub, "user is Unauthorized");
            return response(401, Map.of("message", "Unauthorized"));
            }
            lineNum = 44;
            AddBoycottForm input = objectMapper.readValue(event.getBody(), AddBoycottForm.class);
            input.setUser_id(sub);
            String companyId = input.getCompany_id();
            String companyName = input.getCompany_name();
            List<AddBoycottForm.Reason> reasons = input.getReasons();
            String personalReason = input.getPersonal_reason();
            // validate company_id
            CompanyValidator companyValidator = new CompanyValidator(this.dynamoDb,"companies");
            lineNum = 53;
            boolean validCompany = companyValidator.validateCompanyName(companyId,companyName);
            lineNum = 55;
            if(!validCompany) {
                throw new RuntimeException("not a valid company!");
            }
            lineNum = 59;
            String now = Instant.now().toString();
            boolean userHasBoycott = userHasAnyBoycott(sub, companyId);
            lineNum = 62;
            boolean anySuccess = false;
            List<String> errors = new ArrayList<>();
            for (AddBoycottForm.Reason reason : reasons) {
                String causeId = reason.getCause_id();
                lineNum = 67;
                if (userHasSpecificBoycott(sub, companyId, causeId)) {
                    continue;
                }
                CauseValidator causeValidator = new CauseValidator(this.dynamoDb,"causes");
                lineNum = 72;
                boolean validCause = causeValidator.validateCauseDescription(causeId, reason.getCause_desc());
                lineNum = 74;
                if(!validCause) {
                    continue;
                }
                List<TransactWriteItem> actions = new ArrayList<>();
                actions.add(TransactWriteItem.builder()
                        .put(Put.builder().tableName("user_boycotts")
                                .item(Map.of(
                                        "user_id", AttributeValue.fromS(sub),
                                        "company_id", AttributeValue.fromS(companyId),
                                        "company_name", AttributeValue.fromS(companyName),
                                        "cause_id", AttributeValue.fromS(causeId),
                                        "cause_desc", AttributeValue.fromS(reason.getCause_desc()),
                                        "company_cause_id", AttributeValue.fromS(companyId+"#"+causeId),
                                        "timestamp", AttributeValue.fromS(now)
                                )).build()).build());
                lineNum = 90;
                if (!userIsFollowingCause(sub, causeId)) {
                    lineNum = 92;
                    actions.add(TransactWriteItem.builder()
                            .put(Put.builder().tableName("user_causes")
                                    .item(Map.of(
                                            "user_id", AttributeValue.fromS(sub),
                                            "cause_id", AttributeValue.fromS(causeId),
                                            "cause_desc", AttributeValue.fromS(reason.getCause_desc()),
                                            "timestamp", AttributeValue.fromS(now)
                                    )).build()).build());
                    lineNum = 101;
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
                    lineNum = 112;
                }
                lineNum = 114;
                if (findCauseCompanyRecord(causeId, companyId)) {
                    lineNum = 116;
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
                    lineNum = 134;
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
                lineNum = 150;
                try {
                    lineNum = 152;
                    dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder()
                            .transactItems(actions).build());
                    anySuccess = true;
                    lineNum = 156;
                } catch (RuntimeException e) {
                    Logger.error(lineNum, sub, "Failed to record boycott for cause: " + causeId + " -> " + e.getMessage());
                    errors.add("Failed to record boycott for cause: " + causeId + " -> " + e.getMessage());
                }
            }
            lineNum = 162;
            if (personalReason != null && !personalReason.isBlank()
                    && !userHasPersonalReason(sub, companyId, personalReason)) {
                lineNum = 165;
                List<TransactWriteItem> actions = List.of(
                        TransactWriteItem.builder()
                                .put(Put.builder()
                                        .tableName("user_boycotts")
                                        .item(Map.of(
                                                "user_id", AttributeValue.fromS(sub),
                                                "company_id", AttributeValue.fromS(companyId),
                                                "company_name", AttributeValue.fromS(companyName),
                                                "company_cause_id", AttributeValue.fromS(personalReason+"#"+companyId),
                                                "timestamp", AttributeValue.fromS(now),
                                                "personal_reason", AttributeValue.fromS(personalReason)
                                        )).build()).build()
                );
                lineNum = 179;
                try {
                    dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder()
                            .transactItems(actions).build());
                    anySuccess = true;
                    lineNum = 184;
                } catch (RuntimeException e) {
                    Logger.error(lineNum, sub,
                            "Failed to record boycott for personal reason: " + personalReason + " -> " + e.getMessage());
                            errors.add("Failed to record boycott for personal reason: " + personalReason + " -> " + e.getMessage());
                }
            }
            lineNum = 191;
            if (!userHasBoycott && anySuccess) {
                lineNum = 193;
                dynamoDb.updateItem(UpdateItemRequest.builder()
                        .tableName("companies")
                        .key(Map.of("company_id", AttributeValue.fromS(companyId)))
                        .updateExpression("SET boycott_count = boycott_count + :inc")
                        .expressionAttributeValues(Map.of(
                                ":inc", AttributeValue.fromN("1")
                        )).build());
            }
            lineNum = 202;
            if (!anySuccess) {
                Logger.error(lineNum, sub,
                        "No new boycotts were recorded. Possible duplicates.");
                return response(409, Map.of("message",
                        "No new boycotts were recorded. Possible duplicates."));
            } else if (!errors.isEmpty()) {
                Logger.error(lineNum, sub,
                        "Some boycotts recorded. Errors: " + objectMapper.writeValueAsString(errors));
                return response(207, Map.of("message",
                        "Some boycotts recorded. Errors: " + objectMapper.writeValueAsString(errors)));
            } else {
                lineNum = 214;
                return response(200, Map.of("message",
                        "All boycotts recorded successfully."));
            }

        } catch (Exception e) {
            Logger.error(lineNum, sub, e.getMessage());
            return response(500,Map.of("error", "Unexpected server error: " + e.getMessage()) );
        }
    }

    private APIGatewayProxyResponseEvent response(int status, Object body) {
        String responseBody = null;
        try {
            responseBody = objectMapper.writeValueAsString(body);
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