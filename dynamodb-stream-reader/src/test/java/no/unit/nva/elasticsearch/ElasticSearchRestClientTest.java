package no.unit.nva.elasticsearch;

import elasticsearch.ElasticSearchRestClient;
import nva.commons.utils.Environment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class ElasticSearchRestClientTest {

    @Test
    @DisplayName("Constructor with HTTPClient parameter")
    public void testConstructorWithParameter() {
        HttpClient httpClient = mock(HttpClient.class);
        Environment environment = mock(Environment.class);
        ElasticSearchRestClient elasticSearchRestClient = new ElasticSearchRestClient(httpClient, environment);
        assertNotNull(elasticSearchRestClient);
    }

}
