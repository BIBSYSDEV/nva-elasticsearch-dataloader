package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);
    private final ObjectMapper objectMapper = JsonUtils.objectMapper;

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {

    }

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        try {
            System.out.println("\tevent: " + objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

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

        Map<String, AttributeValue> keys = streamRecord.getDynamodb().getKeys();
        AttributeValue identifierattribute = keys.get("identifier");
        String identifier = identifierattribute.getS();

        System.out.println("Upserting search index for identifier  " + identifier);

//        String record = streamRecord.toString();
//
////        Map<String, AttributeValue> valueMap = record.getNewImage();
//        Item item = ItemUtils.toItem(record);
//        try {
//
//
//            System.out.println("Upserting search index with " + objectMapper.writeValueAsString(record));
//            System.out.println("Values: "+recordNewImage);
//            logger.debug("Upserting search index with {}", objectMapper.writeValueAsString(record));
//            Publication publication = objectMapper.readValue(objectMapper.writeValueAsString(record), Publication.class);
//            System.out.println("publication:" + publication);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }
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
