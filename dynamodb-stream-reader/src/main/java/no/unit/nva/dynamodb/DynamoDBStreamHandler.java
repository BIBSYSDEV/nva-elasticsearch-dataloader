package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import no.unit.nva.elasticsearch.ElasticSearchIndexDocument;
import no.unit.nva.elasticsearch.ElasticSearchRestClient;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.attempt.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.Map;

import static nva.commons.utils.attempt.Try.attempt;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);

    public static final String ELASTICSEARCH_ENDPOINT_INDEX_KEY = "ELASTICSEARCH_ENDPOINT_INDEX";
    public static final String TARGET_SERVICE_URL_KEY = "TARGET_SERVICE_URL";

    public static final String IDENTIFIER = "identifier";
    public static final String DATE_YEAR = "entityDescription.date.year";
    public static final String DESCRIPTION_MAIN_TITLE = "entityDescription.mainTitle";
    public static final String CONTRIBUTORS_IDENTITY_NAME = "entityDescription.contributors.identity.name";
    public static final String PUBLICATION_TYPE = "type";
    public static final String ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE = "Error processing DynamoDBEvent";
    public static final String SUCCESS_MESSAGE = "200 OK";
    private final ElasticSearchRestClient elasticSearchClient;
    private final String targetServiceUrl;
    private final String elasticSearchEndpointIndex;

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {
        this(new Environment());
    }

    /**
     *  constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler(Environment environment) {
        this(new ElasticSearchRestClient(HttpClient.newHttpClient(), environment), environment);
    }



    /**
     * Constructor for DynamoDBStreamHandler for testing.
     *
     * @param elasticSearchRestClient elasticSearchRestClient to be injected for testing
     */
    @JacocoGenerated
    public DynamoDBStreamHandler(ElasticSearchRestClient elasticSearchRestClient, Environment environment) {
        this.elasticSearchClient = elasticSearchRestClient;
        targetServiceUrl = environment.readEnv(TARGET_SERVICE_URL_KEY);
        elasticSearchEndpointIndex = environment.readEnv(ELASTICSEARCH_ENDPOINT_INDEX_KEY);
    }

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        attempt(() -> processRecordStream(event)).orElseThrow(this::logErrorAndThrowException);
        return SUCCESS_MESSAGE;
    }

    private RuntimeException logErrorAndThrowException(Failure<Void> failure) {
        Exception exception = failure.getException();
        logger.error(ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE, exception);
        throw new RuntimeException(exception);
    }

    private Void processRecordStream(DynamodbEvent event) throws InterruptedException, IOException, URISyntaxException {
        for (DynamodbEvent.DynamodbStreamRecord streamRecord : event.getRecords()) {
            processRecord(streamRecord);
        }
        return null;
    }

    private void processRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) throws
            InterruptedException, IOException, URISyntaxException {
        switch (streamRecord.getEventName()) {
            case "INSERT":
            case "MODIFY":
                upsertSearchIndex(streamRecord);
                break;
            case "REMOVE":
                removeFromSearchIndex(streamRecord);
                break;
            default:
                throw new RuntimeException("Not a known operation");
        }
    }

    private void upsertSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord)
            throws InterruptedException, IOException, URISyntaxException {
        String identifier = getIdentifierFromStreamRecord(streamRecord);
        Map<String, AttributeValue> valueMap = streamRecord.getDynamodb().getNewImage();
        logger.trace("valueMap={}", valueMap.toString());

        DynamoDBEventTransformer eventTransformer = new DynamoDBEventTransformer();

        ElasticSearchIndexDocument document = eventTransformer.parseValueMap(
                elasticSearchEndpointIndex,
                targetServiceUrl,
                identifier,
                valueMap);
        elasticSearchClient.addDocumentToIndex(document);
    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord)
            throws InterruptedException, IOException, URISyntaxException {
        String identifier = getIdentifierFromStreamRecord(streamRecord);
        elasticSearchClient.removeDocumentFromIndex(identifier);
    }

    private String getIdentifierFromStreamRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        return streamRecord.getDynamodb().getKeys().get(IDENTIFIER).getS();
    }

}
