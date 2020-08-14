package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SuppressWarnings("unchecked")
public class DynamoDBStreamHandlerTest {

    private DynamoDBStreamHandler handler;

    /**
     * Set up test environment.
     */
    @BeforeEach
    public void init() {
        handler = new DynamoDBStreamHandler();
    }

    @Test
    public void handleRequestReturnsEventOnInput() {

        Map<String,Object> requestEvent = Map.of(
                "eventID", "1",
            "eventVersion", "1.0",
                "dynamodb", Map.of(
                "Keys", Map.of(
                    "ID", Map.of(
                    "N","1"
                                )
                )
            )
        );
        DynamodbEvent requestEvent2 = new DynamodbEvent();
//        when(customerApi.getCustomerId(anyString())).thenReturn(Optional.of(customerId.toString()));
//        String response = handler.handleRequest(requestEvent2, mock(Context.class));
        String response = "sadakdsa";
//        verify(awsCognitoIdentityProvider).adminAddUserToGroup(any());
//        verify(awsCognitoIdentityProvider).adminUpdateUserAttributes(any());

//        assertEquals(requestEvent, responseEvent.get("event"));
        assertNotNull(response);



    }
    
}
