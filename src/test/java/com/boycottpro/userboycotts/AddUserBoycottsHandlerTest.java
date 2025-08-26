package com.boycottpro.userboycotts;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
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
}