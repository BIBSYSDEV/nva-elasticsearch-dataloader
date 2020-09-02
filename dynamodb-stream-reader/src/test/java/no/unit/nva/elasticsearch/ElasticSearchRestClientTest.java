package no.unit.nva.elasticsearch;

import no.unit.nva.dynamodb.DynamoDBStreamHandler;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ElasticSearchRestClientTest {

    @Test
    @DisplayName("testConstructorWithParameters")
    public void testConstructorWithParameterEnvironmentDefined() {
        HttpClient httpClient = mock(HttpClient.class);
        Environment environment = mock(Environment.class);
        when(environment.readEnv(ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY)).thenReturn("localhost");
        when(environment.readEnv(DynamoDBStreamHandler.ELASTICSEARCH_ENDPOINT_INDEX_KEY)).thenReturn("resources");
        when(environment.readEnv(ElasticSearchRestClient.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY)).thenReturn("http");
        ElasticSearchRestClient elasticSearchRestClient = new ElasticSearchRestClient(httpClient, environment);
        assertNotNull(elasticSearchRestClient);
    }

    @Test
    @DisplayName("testConstructorWithParameterMissingEnvironmentVariablesShouldFail")
    public void testConstructorWithParameterMissingEnvironmentVariablesShouldFail() {
        HttpClient httpClient = mock(HttpClient.class);
        Environment environment = spy(Environment.class);
        assertThrows(IllegalStateException.class, () -> new ElasticSearchRestClient(httpClient, environment));
    }


}
