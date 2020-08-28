package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import elasticsearch.ElasticSearchRestClient;
import nva.commons.utils.Environment;
import nva.commons.utils.IoUtils;
import nva.commons.utils.JsonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@SuppressWarnings("unchecked")
public class DynamoDBStreamHandlerTest {

    private static final String SAMPLE_MODIFY_EVENT_FILENAME = "DynamoDBStreamModifyEvent.json";
    private static final String SAMPLE_INSERT_EVENT_FILENAME = "DynamoDBStreamInsertEvent.json";
    private static final String SAMPLE_REMOVE_EVENT_FILENAME = "DynamoDBStreamRemoveEvent.json";
    private static final String SAMPLE_UNKNOWN_EVENT_FILENAME = "UnknownDynamoDBEvent.json";
    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS = "localhost";
    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";
    private static final String ELASTICSEARCH_ENDPOINT_API_SCHEME = "http";
    private static final Object TARGET_SERVICE_URL = "http://localhost/service/";

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
        environment = spy(Environment.class);
        context = mock(Context.class);
        httpClient = spy(HttpClient.class);
        doReturn(ELASTICSEARCH_ENDPOINT_ADDRESS).when(environment)
                .readEnv(ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_INDEX).when(environment)
                .readEnv(DynamoDBStreamHandler.ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_API_SCHEME).when(environment)
                .readEnv(ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY);

        doReturn(TARGET_SERVICE_URL).when(environment)
                .readEnv(DynamoDBStreamHandler.TARGET_SERVICE_URL_KEY);
        doReturn(ELASTICSEARCH_ENDPOINT_INDEX).when(environment)
                .readEnv(DynamoDBStreamHandler.ELASTICSEARCH_ENDPOINT_INDEX_KEY);

        HttpResponse<String> successResponse = mock(HttpResponse.class);
        doReturn(successResponse).when(httpClient).send(any(), any());
        elasticSearchRestClient = new ElasticSearchRestClient(httpClient, environment);
        handler = new DynamoDBStreamHandler(elasticSearchRestClient, environment);
    }

    @Test
    @DisplayName("testHandlerHandleSimpleModifyEventWithoutProblem")
    public void testHandlerHandleSimpleModifyEventWithoutProblem() throws IOException {
        DynamodbEvent requestEvent = loadEventFromResourceFile(SAMPLE_MODIFY_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, context);
        assertNotNull(response);
    }

    @Test
    @DisplayName("testHandlerHandleSimpleInsertEventWithoutProblem")
    public void testHandlerHandleSimpleInsertEventWithoutProblem() throws IOException {
        DynamodbEvent requestEvent = loadEventFromResourceFile(SAMPLE_INSERT_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, context);
        assertNotNull(response);
    }


    @Test
    @DisplayName("testHandlerHandleSimpleRemoveEventWithoutProblem")
    public void testHandlerHandleSimpleRemoveEventWithoutProblem() throws IOException {
        DynamodbEvent requestEvent = loadEventFromResourceFile(SAMPLE_REMOVE_EVENT_FILENAME);
        String response =  handler.handleRequest(requestEvent, context);
        assertNotNull(response);
    }

    @Test
    @DisplayName("testHandleUnknownEventToGiveException")
    public void testHandleUnknownEventToGiveException() throws IOException {
        DynamodbEvent requestEvent = loadEventFromResourceFile(SAMPLE_UNKNOWN_EVENT_FILENAME);
        assertThrows(RuntimeException.class, () -> handler.handleRequest(requestEvent, context));
    }

    @Test
    @DisplayName("testHandleExceptionInEventHandlingShouldGiveException")
    public void testHandleExceptionInEventHandlingShouldGiveException() throws IOException, InterruptedException {
        DynamodbEvent requestEvent = loadEventFromResourceFile(SAMPLE_MODIFY_EVENT_FILENAME);
        doThrow(IOException.class).when(httpClient).send(any(), any());
        assertThrows(RuntimeException.class, () -> handler.handleRequest(requestEvent, context));
    }

    private DynamodbEvent loadEventFromResourceFile(String filename) throws IOException {
        InputStream is = IoUtils.inputStreamFromResources(Paths.get(filename));
        return JsonUtils.objectMapper.readValue(is, DynamodbEvent.class);
    }


}
