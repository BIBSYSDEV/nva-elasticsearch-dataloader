package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
public class DynamoDBStreamHandlerTest {

    private static final String SAMPLE_EVENT_FILENAME = "DynamoDBStreamModifyEvent.json";
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

        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, mock(Context.class));
        assertNotNull(response);


    }

}
