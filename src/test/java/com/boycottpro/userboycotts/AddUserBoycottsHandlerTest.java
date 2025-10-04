package com.boycottpro.userboycotts;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.boycottpro.models.ResponseMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import java.lang.reflect.Field;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.core.JsonProcessingException;

@ExtendWith(MockitoExtension.class)
public class AddUserBoycottsHandlerTest {

    @Mock
    private DynamoDbClient dynamoDbMock;

    @InjectMocks
    private AddUserBoycottsHandler handler;

    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testAllNewBoycottsRecorded() throws Exception {
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" },
                  { "cause_id": "c2", "cause_desc": "Environmental issues" }
                ],
              "personal_reason": "reason"
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        Map<String, AttributeValue> cause2 = Map.of(
                "cause_desc", AttributeValue.fromS("Environmental issues")
        );
        GetItemResponse cause2Response = GetItemResponse.builder()
                .item(cause2)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class))).thenReturn(companyResponse)
                        .thenReturn(cause1Response)
                                .thenReturn(cause2Response);
        when(dynamoDbMock.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().count(0).build());
        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        when(dynamoDbMock.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());

        var response = handler.handleRequest(event, mock(Context.class));
        ResponseMessage message = objectMapper.readValue(response.getBody(), ResponseMessage.class);
        assertEquals(200, response.getStatusCode());
    }

    @Test
    public void testAllDuplicates() throws Exception {
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
                  ],
              "personal_reason": "already exists"
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class))).thenReturn(companyResponse)
                .thenReturn(cause1Response);
        when(dynamoDbMock.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().count(1).build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode());
    }

    @Test
    public void testAllFailures() throws Exception {
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
                  ],
              "personal_reason": "already exists"
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class))).thenReturn(companyResponse)
                .thenReturn(cause1Response);
        when(dynamoDbMock.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().count(0).build());
        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenThrow(new RuntimeException("Failed transaction", null));
        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(409, response.getStatusCode());
    }

    @Test
    public void testPartialSuccess() throws Exception {
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" },
                  { "cause_id": "c2", "cause_desc": "Environmental issues" }
                ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);
        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        Map<String, AttributeValue> cause2 = Map.of(
                "cause_desc", AttributeValue.fromS("Environmental issues")
        );
        GetItemResponse cause2Response = GetItemResponse.builder()
                .item(cause2)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class))).thenReturn(companyResponse)
                .thenReturn(cause1Response)
                .thenReturn(cause2Response);
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build());

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build())  // Success for c1
                .thenThrow(new RuntimeException("Failed transaction", null)); // Fail for c2

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(207, response.getStatusCode());
    }

    @Test
    public void testDefaultConstructor() {
        // Test the default constructor coverage
        // Note: This may fail in environments without AWS credentials/region configured
        try {
            AddUserBoycottsHandler handler = new AddUserBoycottsHandler();
            assertNotNull(handler);

            // Verify DynamoDbClient was created (using reflection to access private field)
            try {
                Field dynamoDbField = AddUserBoycottsHandler.class.getDeclaredField("dynamoDb");
                dynamoDbField.setAccessible(true);
                DynamoDbClient dynamoDb = (DynamoDbClient) dynamoDbField.get(handler);
                assertNotNull(dynamoDb);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                fail("Failed to access DynamoDbClient field: " + e.getMessage());
            }
        } catch (software.amazon.awssdk.core.exception.SdkClientException e) {
            // AWS SDK can't initialize due to missing region configuration
            // This is expected in Jenkins without AWS credentials - test passes
            System.out.println("Skipping DynamoDbClient verification due to AWS SDK configuration: " + e.getMessage());
        }
    }

    @Test
    public void testUnauthorizedUser() {
        // Test the unauthorized block coverage
        handler = new AddUserBoycottsHandler(dynamoDbMock);

        // Create event without JWT token (or invalid token that returns null sub)
        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent();
        // No authorizer context, so JwtUtility.getSubFromRestEvent will return null

        APIGatewayProxyResponseEvent response = handler.handleRequest(event, null);

        assertEquals(401, response.getStatusCode());
        assertTrue(response.getBody().contains("Unauthorized"));
    }

    @Test
    public void testJsonProcessingExceptionInResponse() throws Exception {
        // Test JsonProcessingException coverage in response method by using reflection
        handler = new AddUserBoycottsHandler(dynamoDbMock);

        // Use reflection to access the private response method
        java.lang.reflect.Method responseMethod = AddUserBoycottsHandler.class.getDeclaredMethod("response", int.class, Object.class);
        responseMethod.setAccessible(true);

        // Create an object that will cause JsonProcessingException
        Object problematicObject = new Object() {
            public Object writeReplace() throws java.io.ObjectStreamException {
                throw new java.io.NotSerializableException("Not serializable");
            }
        };

        // Create a circular reference object that will cause JsonProcessingException
        Map<String, Object> circularMap = new HashMap<>();
        circularMap.put("self", circularMap);

        // This should trigger the JsonProcessingException -> RuntimeException path
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            try {
                responseMethod.invoke(handler, 500, circularMap);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                }
                throw new RuntimeException(e.getCause());
            }
        });

        // Verify it's ultimately caused by JsonProcessingException
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof JsonProcessingException,
                "Expected JsonProcessingException, got: " + cause.getClass().getSimpleName());
    }

    @Test
    public void testInvalidCompany() throws Exception {
        // Test lines 56-57: Invalid company validation
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "wrong name",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        // Mock company with different name (validation will fail)
        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("different company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class))).thenReturn(companyResponse);

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(500, response.getStatusCode());
        assertTrue(response.getBody().contains("not a valid company"));
    }

    @Test
    public void testUserAlreadyHasSpecificBoycott() throws Exception {
        // Test lines 68-69: User already has specific boycott
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response);

        // Mock to simulate existing specific boycott
        Map<String, AttributeValue> specificBoycott = Map.of(
                "user_id", AttributeValue.fromS("11111111-2222-3333-4444-555555555555"),
                "company_cause_id", AttributeValue.fromS("comp1#c1")
        );
        QueryResponse specificBoycottResponse = QueryResponse.builder()
                .items(specificBoycott)
                .build();

        // First query: userHasAnyBoycott (returns empty - no boycott yet)
        // Second query: userHasSpecificBoycott (returns specific boycott)
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build())  // userHasAnyBoycott
                .thenReturn(specificBoycottResponse); // userHasSpecificBoycott

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(409, response.getStatusCode()); // No new boycotts recorded
        assertTrue(response.getBody().contains("No new boycotts were recorded"));
    }

    @Test
    public void testInvalidCause() throws Exception {
        // Test lines 75-76: Invalid cause validation
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Wrong description" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Different description")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response);

        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(409, response.getStatusCode()); // No boycotts recorded due to invalid cause
    }

    @Test
    public void testUserAlreadyFollowingCause() throws Exception {
        // Test lines 91, 115: User already following cause, cause_company_stats record exists
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();

        // Mock for findCauseCompanyRecord (returns existing record)
        Map<String, AttributeValue> causeCompanyRecord = Map.of(
                "cause_id", AttributeValue.fromS("c1"),
                "company_id", AttributeValue.fromS("comp1")
        );
        GetItemResponse causeCompanyResponse = GetItemResponse.builder()
                .item(causeCompanyRecord)
                .build();

        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response)
                .thenReturn(causeCompanyResponse); // findCauseCompanyRecord returns true

        // First query: userHasAnyBoycott
        // Second query: userHasSpecificBoycott (returns false)
        // Third query: userIsFollowingCause (returns true - already following)
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build())  // userHasAnyBoycott
                .thenReturn(QueryResponse.builder().count(0).build())  // userHasSpecificBoycott
                .thenReturn(QueryResponse.builder().count(1).build()); // userIsFollowingCause

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        when(dynamoDbMock.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode());
    }

    @Test
    public void testCauseCompanyRecordDoesNotExist() throws Exception {
        // Test lines 134-147: Create new cause_company_stats record
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();

        // Mock for findCauseCompanyRecord (returns no item - record doesn't exist)
        GetItemResponse noCauseCompanyResponse = GetItemResponse.builder().build();

        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response)
                .thenReturn(noCauseCompanyResponse); // findCauseCompanyRecord returns false

        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build());

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        when(dynamoDbMock.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode());
    }

    @Test
    public void testPersonalReasonNotBlankAndNew() throws Exception {
        // Test lines 163-164, 192: Personal reason is not blank and user doesn't have it yet
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [],
              "personal_reason": "My personal reason"
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class))).thenReturn(companyResponse);

        // First query: userHasAnyBoycott (returns false - no boycott yet)
        // Second query: userHasPersonalReason (returns false - no personal reason yet)
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build())
                .thenReturn(QueryResponse.builder().count(0).build());

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        when(dynamoDbMock.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode());
    }

    @Test
    public void testFindCauseCompanyRecordException() throws Exception {
        // Test lines 251-253: DynamoDbException in findCauseCompanyRecord
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();

        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response)
                .thenThrow(DynamoDbException.builder().message("DynamoDB error").build()); // findCauseCompanyRecord throws exception

        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build());

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        when(dynamoDbMock.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode()); // Should still succeed, findCauseCompanyRecord returns false on exception
    }

    @Test
    public void testUserHasAnyBoycottException() throws Exception {
        // Test lines 272-274: DynamoDbException in userHasAnyBoycott
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        GetItemResponse noCauseCompanyResponse = GetItemResponse.builder().build();

        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response)
                .thenReturn(noCauseCompanyResponse);

        // First query: userHasAnyBoycott throws DynamoDbException
        // Second query: userHasSpecificBoycott
        // Third query: userIsFollowingCause
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenThrow(DynamoDbException.builder().message("DynamoDB error").build()) // userHasAnyBoycott exception
                .thenReturn(QueryResponse.builder().count(0).build()) // userHasSpecificBoycott
                .thenReturn(QueryResponse.builder().count(0).build()); // userIsFollowingCause

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode()); // Should still succeed
    }

    @Test
    public void testUserHasPersonalReasonException() throws Exception {
        // Test lines 312-315: DynamoDbException in userHasPersonalReason
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [],
              "personal_reason": "My personal reason"
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class))).thenReturn(companyResponse);

        // First query: userHasAnyBoycott
        // Second query: userHasPersonalReason throws exception
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build()) // userHasAnyBoycott
                .thenThrow(DynamoDbException.builder().message("DynamoDB error").build()); // userHasPersonalReason exception

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        when(dynamoDbMock.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode()); // Should still succeed, userHasPersonalReason returns false on exception
    }

    @Test
    public void testUserHasPersonalReasonWithMatch() throws Exception {
        // Test lines 303-307: userHasPersonalReason finds matching personal reason
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [],
              "personal_reason": "Existing reason"
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        when(dynamoDbMock.getItem(any(GetItemRequest.class))).thenReturn(companyResponse);

        // Mock userHasPersonalReason to return a matching record
        Map<String, AttributeValue> personalReasonRecord = Map.of(
                "user_id", AttributeValue.fromS("11111111-2222-3333-4444-555555555555"),
                "company_id", AttributeValue.fromS("comp1"),
                "personal_reason", AttributeValue.fromS("Existing reason")
        );
        QueryResponse personalReasonResponse = QueryResponse.builder()
                .items(personalReasonRecord)
                .build();

        // First query: userHasAnyBoycott
        // Second query: userHasPersonalReason (returns matching personal reason)
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build()) // userHasAnyBoycott
                .thenReturn(personalReasonResponse); // userHasPersonalReason returns match

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(409, response.getStatusCode()); // No new boycotts recorded
    }

    @Test
    public void testUserHasAnyBoycottMatches() throws Exception {
        // Test line 270: userHasAnyBoycott finds matching company_id
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        GetItemResponse noCauseCompanyResponse = GetItemResponse.builder().build();

        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response)
                .thenReturn(noCauseCompanyResponse);

        // Mock userHasAnyBoycott to return matching company_id
        Map<String, AttributeValue> existingBoycott = Map.of(
                "user_id", AttributeValue.fromS("11111111-2222-3333-4444-555555555555"),
                "company_id", AttributeValue.fromS("comp1")
        );
        QueryResponse userBoycottResponse = QueryResponse.builder()
                .items(existingBoycott)
                .build();

        // First query: userHasAnyBoycott (returns matching boycott)
        // Second query: userHasSpecificBoycott
        // Third query: userIsFollowingCause
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(userBoycottResponse) // userHasAnyBoycott returns match
                .thenReturn(QueryResponse.builder().count(0).build()) // userHasSpecificBoycott
                .thenReturn(QueryResponse.builder().count(0).build()); // userIsFollowingCause

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        // updateItem should NOT be called since userHasBoycott is true

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode());
        // Verify updateItem was NOT called
        verify(dynamoDbMock, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    public void testUserIsFollowingCauseMatches() throws Exception {
        // Test line 331: userIsFollowingCause finds matching cause
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();
        GetItemResponse causeCompanyResponse = GetItemResponse.builder()
                .item(Map.of("cause_id", AttributeValue.fromS("c1")))
                .build();

        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response)
                .thenReturn(causeCompanyResponse);

        // Mock userIsFollowingCause to return matching cause
        Map<String, AttributeValue> followingCause = Map.of(
                "user_id", AttributeValue.fromS("11111111-2222-3333-4444-555555555555"),
                "cause_id", AttributeValue.fromS("c1")
        );
        QueryResponse followingResponse = QueryResponse.builder()
                .items(followingCause)
                .build();

        // First query: userHasAnyBoycott
        // Second query: userHasSpecificBoycott
        // Third query: userIsFollowingCause (returns match)
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build()) // userHasAnyBoycott
                .thenReturn(QueryResponse.builder().count(0).build()) // userHasSpecificBoycott
                .thenReturn(followingResponse); // userIsFollowingCause returns match

        when(dynamoDbMock.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        when(dynamoDbMock.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(200, response.getStatusCode());
    }

    @Test
    public void testGeneralExceptionHandling() throws Exception {
        // Test lines 219-221: General exception handling
        String invalidJson = "{ invalid json";

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(invalidJson);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(500, response.getStatusCode());
        assertTrue(response.getBody().contains("Unexpected server error"));
    }

    @Test
    public void testUserHasSpecificBoycottMatches() throws Exception {
        // Test line 290: userHasSpecificBoycott returns non-empty items
        // This is already covered by testUserAlreadyHasSpecificBoycott above
        // Just ensuring explicit coverage of line 290
        String body = """
            {
              "user_id": null,
              "company_id": "comp1",
              "company_name" : "this company",
              "reasons": [
                  { "cause_id": "c1", "cause_desc": "Labor rights" }
              ],
              "personal_reason": null
            }
        """;

        APIGatewayProxyRequestEvent event = new APIGatewayProxyRequestEvent().withBody(body);
        Map<String, String> claims = Map.of("sub", "11111111-2222-3333-4444-555555555555");
        Map<String, Object> authorizer = new HashMap<>();
        authorizer.put("claims", claims);

        APIGatewayProxyRequestEvent.ProxyRequestContext rc = new APIGatewayProxyRequestEvent.ProxyRequestContext();
        rc.setAuthorizer(authorizer);
        event.setRequestContext(rc);

        Map<String, AttributeValue> company = Map.of(
                "company_name", AttributeValue.fromS("this company")
        );
        GetItemResponse companyResponse = GetItemResponse.builder()
                .item(company)
                .build();
        Map<String, AttributeValue> cause1 = Map.of(
                "cause_desc", AttributeValue.fromS("Labor rights")
        );
        GetItemResponse cause1Response = GetItemResponse.builder()
                .item(cause1)
                .build();

        when(dynamoDbMock.getItem(any(GetItemRequest.class)))
                .thenReturn(companyResponse)
                .thenReturn(cause1Response);

        // Mock to simulate existing specific boycott
        Map<String, AttributeValue> specificBoycott = Map.of(
                "user_id", AttributeValue.fromS("11111111-2222-3333-4444-555555555555"),
                "company_cause_id", AttributeValue.fromS("comp1#c1")
        );
        QueryResponse specificBoycottResponse = QueryResponse.builder()
                .items(specificBoycott)
                .build();

        // First query: userHasAnyBoycott
        // Second query: userHasSpecificBoycott (returns specific boycott)
        when(dynamoDbMock.query(any(QueryRequest.class)))
                .thenReturn(QueryResponse.builder().count(0).build()) // userHasAnyBoycott
                .thenReturn(specificBoycottResponse); // userHasSpecificBoycott returns item

        var response = handler.handleRequest(event, mock(Context.class));
        assertEquals(409, response.getStatusCode());
    }

}