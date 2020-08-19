package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import elasticsearch.ElasticSearchRestClient;
import nva.commons.utils.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    public static final String IDENTIFIER = "identifier";

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);
    private final ElasticSearchRestClient elasticSearchClient = new ElasticSearchRestClient();

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {

    }

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        for (DynamodbEvent.DynamodbStreamRecord streamRecord : event.getRecords()) {
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
        return "Handled " + event.getRecords().size() + " records";
    }

    private void upsertSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        String identifier = streamRecord.getDynamodb().getKeys().get(IDENTIFIER).getS();
        Map<String, AttributeValue> valueMap = streamRecord.getDynamodb().getNewImage();
        ValueMapFlattener flattener = new ValueMapFlattener.Builder()
                .withSeparator(".")
                .build();
        PublicationIndexDocument flattenedPublication = flattener.flattenValueMap(identifier, valueMap);
        logger.trace("Upserting search index for identifier {} with values {}",identifier, flattenedPublication);
        elasticSearchClient.addDocumentToIndex(flattenedPublication);
    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        String identifier = streamRecord.getDynamodb().getKeys().get(IDENTIFIER).getS();
        logger.trace("Deleting from search API publication with identifier: {}", identifier);
        elasticSearchClient.removeDocumentFromIndex(identifier);
    }




}
