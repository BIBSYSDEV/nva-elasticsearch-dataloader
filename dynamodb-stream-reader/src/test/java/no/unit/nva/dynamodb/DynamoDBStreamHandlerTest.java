package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
public class DynamoDBStreamHandlerTest {

    private static final String SAMPLE_MODIFY_EVENT_FILENAME = "DynamoDBStreamModifyEvent.json";
    private static final String SAMPLE_INSERT_EVENT_FILENAME = "DynamoDBStreamInsertEvent.json";
    private static final String SAMPLE_REMOVE_EVENT_FILENAME = "DynamoDBStreamRemoveEvent.json";
    private static final String SAMPLE_UNKNOWN_EVENT_FILENAME = "UnknownDynamoDBEvent.json";

    private DynamoDBStreamHandler handler;

    /**
     * Set up test environment.
     */
    @BeforeEach
    public void init() {
        handler = new DynamoDBStreamHandler();
    }

    @Test
    @DisplayName("MODIFY DynamoDBStreamEvent")
    public void handleModifyEvent() {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_MODIFY_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, mock(Context.class));
        assertNotNull(response);
    }

    @Test
    @DisplayName("INSERT DynamoDBStreamEvent")
    public void handleInsertEvent() {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_INSERT_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, mock(Context.class));
        assertNotNull(response);
    }


    @Test
    @DisplayName("REMOVE DynamoDBStreamEvent")
    public void handleRemoveEvent() {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_REMOVE_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, mock(Context.class));
        assertNotNull(response);
    }

    @Test
    @DisplayName("DynamoDBStreamEvent without correct operation")
    public void handleUnknownEvent() {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_UNKNOWN_EVENT_FILENAME);
        assertThrows(RuntimeException.class, () -> handler.handleRequest(requestEvent, mock(Context.class)));
    }



}
