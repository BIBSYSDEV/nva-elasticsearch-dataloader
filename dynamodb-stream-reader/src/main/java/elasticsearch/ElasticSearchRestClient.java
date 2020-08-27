package elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import nva.commons.utils.Environment;
import nva.commons.utils.JsonUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static no.unit.nva.dynamodb.ValueMapFlattener.IDENTIFIER_KEY;

public class ElasticSearchRestClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    private static final String ELASTICSEARCH_ENDPOINT_OPERATION = "_doc";
    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} {} and index {}";
    public static final String APPLICATION_JSON = "application/json";

    public static final String UPSERTING_LOG_MESSAGE = "Upserting search index  with values {}";
    public static final String DELETE_LOG_MESSAGE = "Deleting from search API publication with identifier: {}";
    public static final String ELASTICSEARCH_ENDPOINT_INDEX_KEY = "ELASTICSEARCH_ENDPOINT_INDEX";
    public static final String ELASTICSEARCH_ENDPOINT_ADDRESS_KEY = "ELASTICSEARCH_ENDPOINT_ADDRESS";
    public static final String ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY = "ELASTICSEARCH_ENDPOINT_API_SCHEME";
    public static final String ELASTICSEARCH_ENDPOINT_URI_TEMPLATE = "%s://%s/%s/%s/%s";
    public static final String POSTING_TO_ENDPOINT_LOG_MESSAGE = "POSTing {} to endpoint {}";
    public static final String MISSING_IN_ENVIRONMENT_ERROR = "Missing '%s' in environment";

    private final HttpClient client;
    private final String elasticSearchEndpointAddress;
    private final String elasticSearchEndpointIndex;
    private final String elasticSearchEndpointScheme;

    /**
     * Creates a new ElasticSearchRestClient.
     *
     * @param httpClient Client to speak http
     * @param environment Environment with properties
     */
    public ElasticSearchRestClient(HttpClient httpClient, Environment environment) {
        client = httpClient;
        elasticSearchEndpointAddress = environment.readEnv(ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        if (elasticSearchEndpointAddress == null) {
            throw new IllegalArgumentException(
                    String.format(MISSING_IN_ENVIRONMENT_ERROR, "elasticSearchEndpointAddress"));
        }
        elasticSearchEndpointIndex = environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        if (elasticSearchEndpointIndex == null) {
            throw new IllegalArgumentException(
                    String.format(MISSING_IN_ENVIRONMENT_ERROR, "elasticSearchEndpointIndex"));
        }
        elasticSearchEndpointScheme = environment.readEnv(ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY);
        if (elasticSearchEndpointScheme == null) {
            throw new IllegalArgumentException(
                    String.format(MISSING_IN_ENVIRONMENT_ERROR, "elasticSearchEndpointScheme"));
        }

        logger.info(INITIAL_LOG_MESSAGE,
                elasticSearchEndpointScheme, elasticSearchEndpointAddress, elasticSearchEndpointIndex);
    }

    /**
     * Adds or insert a document to an elasticsearch index.
     * @param document the document to be inserted
     * @throws URISyntaxException thrown when uri is misconfigured
     * @throws IOException thrown hen service i not available
     * @throws InterruptedException thrown when service i interrupted
     */
    public void addDocumentToIndex(Map<String, String> document)
            throws URISyntaxException, IOException, InterruptedException {
        logger.debug(UPSERTING_LOG_MESSAGE, document);

        HttpRequest request = createHttpRequest(document);

        HttpResponse<String> response = doSend(request);
        logger.debug(response.body());
    }

    private HttpRequest createHttpRequest(Map<String, String> document) throws
            JsonProcessingException, URISyntaxException {
        String requestBody = JsonUtils.objectMapper.writeValueAsString(document);
        String identifier = document.get(IDENTIFIER_KEY);

        HttpRequest request = buildHttpRequest(requestBody, identifier);

        logger.debug(POSTING_TO_ENDPOINT_LOG_MESSAGE, requestBody, createUpsertDocumentURI(identifier));
        return request;
    }

    private HttpRequest buildHttpRequest(String requestBody, String identifier) throws URISyntaxException {
        return HttpRequest.newBuilder()
                .uri(createUpsertDocumentURI(identifier))
                .header(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody)) // GET is default
                .build();
    }

    /**
     * Removes an indexed document from elasticsearch index.
     * @param identifier identifier of document to remove from elasticsearch
     * @throws URISyntaxException thrown when uri is misconfigured
     * @throws IOException thrown hen service i not available
     * @throws InterruptedException thrown when service i interrupted
     */
    public void removeDocumentFromIndex(String identifier)
            throws URISyntaxException, IOException, InterruptedException {
        logger.trace(DELETE_LOG_MESSAGE, identifier);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(createUpsertDocumentURI(identifier))
                .header(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .DELETE()
                .build();

        logger.debug("DELETEing {} ", createUpsertDocumentURI(identifier));
        HttpResponse<String> response = doSend(request);
        logger.debug(response.body());
    }

    private HttpResponse<String> doSend(HttpRequest request) throws IOException, InterruptedException {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private URI createUpsertDocumentURI(String identifier) throws URISyntaxException {
        String uriString = String.format(ELASTICSEARCH_ENDPOINT_URI_TEMPLATE,
                elasticSearchEndpointScheme, elasticSearchEndpointAddress,
                elasticSearchEndpointIndex, ELASTICSEARCH_ENDPOINT_OPERATION, identifier);
        return URI.create(uriString);
    }

}
