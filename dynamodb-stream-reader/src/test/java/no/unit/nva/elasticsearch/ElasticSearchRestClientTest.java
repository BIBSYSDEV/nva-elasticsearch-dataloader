package no.unit.nva.elasticsearch;

import elasticsearch.ElasticSearchRestClient;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticSearchRestClientTest {

    @Test
    @DisplayName("Constructor with HTTPClient parameter")
    public void testConstructorWithParameter() {
        HttpClient httpClient = mock(HttpClient.class);
        Environment environment = mock(Environment.class);
        ElasticSearchRestClient elasticSearchRestClient = new ElasticSearchRestClient(httpClient, environment);
        assertNotNull(elasticSearchRestClient);
    }

    @Test
    @DisplayName("doSend")
    public void testDoSend() throws IOException, InterruptedException {
        HttpClient httpClient = mock(HttpClient.class);
        Environment environment = mock(Environment.class);
        ElasticSearchRestClient elasticSearchRestClient = new ElasticSearchRestClient(httpClient, environment);
        assertNotNull(elasticSearchRestClient);
        HttpRequest request = mock(HttpRequest.class);
        HttpResponse.BodyHandler bodyHandler = mock(HttpResponse.BodyHandler.class);
        when(httpClient.send(request, bodyHandler)).thenReturn(mock(HttpResponse.class));
        HttpResponse<String> response = elasticSearchRestClient.doSend(request);
    }

}
