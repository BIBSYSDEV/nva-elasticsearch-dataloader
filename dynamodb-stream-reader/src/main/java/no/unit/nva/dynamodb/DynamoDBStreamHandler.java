package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nva.commons.utils.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {

    }

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        for (DynamodbEvent.DynamodbStreamRecord streamRecord: event.getRecords()) {
            switch (streamRecord.getEventName()) {
                case "INSERT":
                case "MODIFY" : upsertSearchIndex(streamRecord);
                                break;
                case "REMOVE" : removeFromSearchIndex(streamRecord);
                                break;
                default:        throw  new RuntimeException("Not a known operation");
            }
        }
        return "Handled " + event.getRecords().size() + " records";
    }

    private void upsertSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        StreamRecord record = streamRecord.getDynamodb();
        try {
            System.out.println("Upserting search index with " + objectMapper.writeValueAsString(record));
            logger.debug("Upserting search index with {}", objectMapper.writeValueAsString(record));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        StreamRecord record = streamRecord.getDynamodb();
        try {
            System.out.println("deleteing from search index  " + objectMapper.writeValueAsString(record));
            logger.debug("Deleteing from search index {}", objectMapper.writeValueAsString(record));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
