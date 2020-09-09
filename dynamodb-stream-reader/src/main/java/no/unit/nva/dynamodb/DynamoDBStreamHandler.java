package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import no.unit.nva.elasticsearch.Constants;
import no.unit.nva.elasticsearch.ElasticSearchRestClient;
import no.unit.nva.elasticsearch.IndexDocument;
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

    public static final String ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE = "Error processing DynamoDBEvent";
    public static final String SUCCESS_MESSAGE = "200 OK";

    private final ElasticSearchRestClient elasticSearchClient;

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
        this(new ElasticSearchRestClient(HttpClient.newHttpClient(), environment));
    }



    /**
     * Constructor for DynamoDBStreamHandler for testing.
     *
     * @param elasticSearchRestClient elasticSearchRestClient to be injected for testing
     */
    @JacocoGenerated
    public DynamoDBStreamHandler(ElasticSearchRestClient elasticSearchRestClient) {
        this.elasticSearchClient = elasticSearchRestClient;
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
            InterruptedException, IOException {
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
            throws InterruptedException, IOException {
        Map<String, AttributeValue> valueMap = streamRecord.getDynamodb().getNewImage();
        logger.trace("valueMap={}", valueMap.toString());

        DynamoDBEventTransformer eventTransformer = new DynamoDBEventTransformer();

        IndexDocument document = eventTransformer.parseStreamRecord(streamRecord);
        elasticSearchClient.addDocumentToIndex(document);
    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord)
            throws InterruptedException, IOException {
        String identifier = getIdentifierFromStreamRecord(streamRecord);
        elasticSearchClient.removeDocumentFromIndex(identifier);
    }

    private String getIdentifierFromStreamRecord(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        return streamRecord.getDynamodb().getKeys().get(Constants.IDENTIFIER).getS();
    }
}
