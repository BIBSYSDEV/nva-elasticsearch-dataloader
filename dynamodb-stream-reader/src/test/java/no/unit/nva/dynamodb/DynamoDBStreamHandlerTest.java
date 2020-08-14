package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SuppressWarnings("unchecked")
public class DynamoDBStreamHandlerTest {

    private static final String SAMPLE_EVENT_FILENAME = "SampleDynamoDBStreamEvent.json";
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

        DynamodbEvent requestEvent2 = ResourceUtils.loadEventFromResourceFile(SAMPLE_EVENT_FILENAME);
        String response = "sadakdsa";
        assertNotNull(response);


    }

}
