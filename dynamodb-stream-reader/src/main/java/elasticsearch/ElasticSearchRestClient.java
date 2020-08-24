package elasticsearch;

import no.unit.nva.dynamodb.PublicationIndexDocument;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.StringJoiner;

public class ElasticSearchRestClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    private static final String ELASTICSEARCH_ENDPOINT_INDEX = "resources";
    private static final String ELASTICSEARCH_ENDPOINT_ADDRESS =
            "http://search-elastic-nvaela-1eycqyjqr5n01-ovx3m2iroxv222s6bu5a7ow3jm.eu-west-1.es.amazonaws.com";
    private static final String ELASTICSEARCH_ENDPOINT_OPERATION = "_doc";

    private  final HttpClient client;

    public ElasticSearchRestClient() {
        this(HttpClient.newHttpClient());
    }

    public ElasticSearchRestClient(HttpClient client) {
        this.client = client;
    }

    /**
     * Adds or insert a document to an elasticsearch index.
     * @param document the document to be inserted
     * @return true if operation is an success
     * @throws URISyntaxException thrown when uri is misconfigured
     * @throws IOException thrown hen service i not available
     * @throws InterruptedException thrown when service i interrupted
     */
    public boolean addDocumentToIndex(PublicationIndexDocument document)
            throws URISyntaxException, IOException, InterruptedException {
        logger.debug("Upserting search index  with values {}", document);

        String requestBody = "";
        requestBody = JsonUtils.objectMapper.writeValueAsString(document);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(createUpsertDocumentURI(document.getIdentifier()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody)) // GET is default
                .build();


        logger.debug("POSTing {} to endpoint {}", requestBody, createUpsertDocumentURI(document.getIdentifier()));
        HttpResponse<String> response = doSend(request);
        logger.debug(response.body());
        return true;
    }

    /**
     * Removes an indexed document from elasticsearch index.
     * @param identifier identifier of document to remove from elasticsearch
     * @return true if operatrion is successful
     * @throws URISyntaxException thrown when uri is misconfigured
     * @throws IOException thrown hen service i not available
     * @throws InterruptedException thrown when service i interrupted
     */
    public boolean removeDocumentFromIndex(String identifier)
            throws URISyntaxException, IOException, InterruptedException {
        logger.trace("Deleting from search API publication with identifier: {}", identifier);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(createUpsertDocumentURI(identifier))
                .header("Content-Type", "application/json")
                .DELETE()
                .build();


        logger.debug("DELETEing {} ", createUpsertDocumentURI(identifier));
        HttpResponse<String> response = doSend(request);
        logger.debug(response.body());

        return true;
    }

    public HttpResponse<String> doSend(HttpRequest request) throws IOException, InterruptedException {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private URI createUpsertDocumentURI(String identifier) throws URISyntaxException {
        StringJoiner stringJoiner = new StringJoiner("/");
        return  new URI(
                stringJoiner.add(ELASTICSEARCH_ENDPOINT_ADDRESS)
                        .add(ELASTICSEARCH_ENDPOINT_INDEX)
                        .add(ELASTICSEARCH_ENDPOINT_OPERATION)
                        .add(identifier)
                        .toString());
    }


}
