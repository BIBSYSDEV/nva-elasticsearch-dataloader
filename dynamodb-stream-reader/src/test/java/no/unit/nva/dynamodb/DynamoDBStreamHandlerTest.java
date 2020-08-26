package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import elasticsearch.ElasticSearchRestClient;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class DynamoDBStreamHandlerTest {

    private static final String SAMPLE_MODIFY_EVENT_FILENAME = "DynamoDBStreamModifyEvent.json";
    private static final String SAMPLE_INSERT_EVENT_FILENAME = "DynamoDBStreamInsertEvent.json";
    private static final String SAMPLE_REMOVE_EVENT_FILENAME = "DynamoDBStreamRemoveEvent.json";
    private static final String SAMPLE_UNKNOWN_EVENT_FILENAME = "UnknownDynamoDBEvent.json";

    private DynamoDBStreamHandler handler;
    private ElasticSearchRestClient elasticSearchRestClient;
    private Environment environment;
    private Context context;
    private HttpClient httpClient;

    /**
     * Set up test environment.
     *
     * @throws IOException some error occurred
     * @throws InterruptedException another error occurred
     */
    @BeforeEach
    public void init() throws IOException, InterruptedException {
        environment = mock(Environment.class);
        context = mock(Context.class);
        httpClient = spy(HttpClient.class);
        when(environment.readEnv(ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn("localhost");
        when(environment.readEnv(ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("http");
        HttpResponse<String> successResponse = mock(HttpResponse.class);
        doReturn(successResponse).when(httpClient).send(any(), any());
        elasticSearchRestClient = new ElasticSearchRestClient(httpClient, environment);
        handler = new DynamoDBStreamHandler(elasticSearchRestClient);
    }

    @Test
    @DisplayName("MODIFY DynamoDBStreamEvent")
    public void handleModifyEvent() throws IOException {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_MODIFY_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, context);
        assertNotNull(response);
    }

    @Test
    @DisplayName("INSERT DynamoDBStreamEvent")
    public void handleInsertEvent() throws IOException {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_INSERT_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, context);
        assertNotNull(response);
    }


    @Test
    @DisplayName("REMOVE DynamoDBStreamEvent")
    public void handleRemoveEvent() throws IOException {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_REMOVE_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, context);
        assertNotNull(response);
    }

    @Test
    @DisplayName("DynamoDBStreamEvent without correct operation")
    public void handleUnknownEvent() throws IOException {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_UNKNOWN_EVENT_FILENAME);
        assertThrows(RuntimeException.class, () -> handler.handleRequest(requestEvent, context));
    }

    @Test
    @DisplayName("Test exception when handling event")
    public void handleExceptionInEventHandling() throws IOException, InterruptedException {
        DynamodbEvent requestEvent = ResourceUtils.loadEventFromResourceFile(SAMPLE_MODIFY_EVENT_FILENAME);
        doThrow(IOException.class).when(httpClient).send(any(), any());
        assertThrows(RuntimeException.class, () -> handler.handleRequest(requestEvent, context));
    }



}
