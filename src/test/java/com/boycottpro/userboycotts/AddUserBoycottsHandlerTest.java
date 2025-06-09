package com.boycottpro.userboycotts;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
public class AddUserBoycottsHandlerTest {

    private static final String TABLE_NAME = "";

    @Mock
    private DynamoDbClient dynamoDbMock;


    private AddUserBoycottsHandler handler;

    @Test
    public void handleRequest() throws Exception {

    }

}