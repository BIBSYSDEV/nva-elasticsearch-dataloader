package no.unit.nva.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import nva.commons.utils.Environment;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static no.unit.nva.elasticsearch.Constants.ELASTICSEARCH_ENDPOINT_INDEX_KEY;

public class ElasticSearchRestClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    public static final String INITIAL_LOG_MESSAGE = "using Elasticsearch endpoint {} {} and index {}";
    public static final String UPSERTING_LOG_MESSAGE = "Upserting search index  with values {}";
    public static final String DELETE_LOG_MESSAGE = "Deleting from search API publication with identifier: {}";
    public static final String POSTING_TO_ENDPOINT_LOG_MESSAGE = "POSTing {} to endpoint {}";

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
        elasticSearchEndpointAddress = environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_ADDRESS_KEY);
        elasticSearchEndpointIndex = environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
        elasticSearchEndpointScheme = environment.readEnv(Constants.ELASTICSEARCH_ENDPOINT_API_SCHEME_KEY);

        logger.info(INITIAL_LOG_MESSAGE,
                elasticSearchEndpointScheme, elasticSearchEndpointAddress, elasticSearchEndpointIndex);
    }

    /**
     * Adds or insert a document to an elasticsearch index.
     * @param document the document to be inserted
     * @throws IOException thrown hen service i not available
     * @throws InterruptedException thrown when service i interrupted
     */
    public void addDocumentToIndex(IndexDocument document)
            throws IOException, InterruptedException {
        logger.debug(UPSERTING_LOG_MESSAGE, document);

        HttpRequest request = createHttpRequest(document);

        HttpResponse<String> response = doSend(request);
        logger.debug(response.body());
    }

    private HttpRequest createHttpRequest(IndexDocument document) throws JsonProcessingException {
        String requestBody = document.toJsonString();
        String identifier = document.getIdentifier();

        HttpRequest request = buildHttpRequest(requestBody, identifier);

        logger.debug(POSTING_TO_ENDPOINT_LOG_MESSAGE, requestBody, createUpsertDocumentURI(identifier));
        return request;
    }

    private HttpRequest buildHttpRequest(String requestBody, String identifier) {
        return HttpRequest.newBuilder()
                .uri(createUpsertDocumentURI(identifier))
                .header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
    }

    /**
     * Removes an indexed document from elasticsearch index.
     * @param identifier identifier of document to remove from elasticsearch
     * @throws IOException thrown hen service i not available
     * @throws InterruptedException thrown when service i interrupted
     */
    public void removeDocumentFromIndex(String identifier)
            throws IOException, InterruptedException {
        logger.trace(DELETE_LOG_MESSAGE, identifier);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(createUpsertDocumentURI(identifier))
                .header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
                .DELETE()
                .build();

        HttpResponse<String> response = doSend(request);
        logger.debug(response.body());
    }

    private HttpResponse<String> doSend(HttpRequest request) throws IOException, InterruptedException {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private URI createUpsertDocumentURI(String identifier) {
        String uriString = String.format(Constants.ELASTICSEARCH_ENDPOINT_URI_TEMPLATE,
                elasticSearchEndpointScheme, elasticSearchEndpointAddress,
                elasticSearchEndpointIndex, Constants.ELASTICSEARCH_ENDPOINT_OPERATION, identifier);
        return URI.create(uriString);
    }

}
