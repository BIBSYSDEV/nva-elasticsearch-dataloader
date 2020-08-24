package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import elasticsearch.ElasticSearchRestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class DynamoDBStreamHandlerTest {

    private static final String SAMPLE_MODIFY_EVENT_FILENAME = "DynamoDBStreamModifyEvent.json";
    private static final String SAMPLE_INSERT_EVENT_FILENAME = "DynamoDBStreamInsertEvent.json";
    private static final String SAMPLE_REMOVE_EVENT_FILENAME = "DynamoDBStreamRemoveEvent.json";
    private static final String SAMPLE_UNKNOWN_EVENT_FILENAME = "UnknownDynamoDBEvent.json";

    private DynamoDBStreamHandler handler;
    private HttpResponse<String> successResponse;
    private ElasticSearchRestClient elasticSearchRestClient;

    /**
     * Set up test environment.
     */
    @BeforeEach
    public void init() {
        elasticSearchRestClient = mock(ElasticSearchRestClient.class);
        successResponse = mock(HttpResponse.class);
        try {
            when(elasticSearchRestClient.addDocumentToIndex(any(PublicationIndexDocument.class))).thenCallRealMethod();
            when(elasticSearchRestClient.removeDocumentFromIndex(anyString())).thenCallRealMethod();
            when(elasticSearchRestClient.doSend(any())).thenReturn(successResponse);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        handler = new DynamoDBStreamHandler(elasticSearchRestClient);
    }

    @Test
    @DisplayName("Default constructor")
    public void testDefaultConstructor() {
        DynamoDBStreamHandler dynamoDBStreamHandler = new DynamoDBStreamHandler();
        assertNotNull(dynamoDBStreamHandler);
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

    @Test
    @DisplayName("DynamoDBStreamHandler remove  exceptionHandling")
    public void handleExceptionWhenRemove() throws IOException, InterruptedException {
        when(elasticSearchRestClient.doSend(any())).thenThrow(new IOException());
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_REMOVE_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, mock(Context.class));
        assertNotNull(response);

    }

    @Test
    @DisplayName("DynamoDBStreamHandler remove  exceptionHandling")
    public void handleExceptionWhenUpsert() throws IOException, InterruptedException {
        when(elasticSearchRestClient.doSend(any())).thenThrow(new IOException());
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_INSERT_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, mock(Context.class));
        assertNotNull(response);

    }

}
