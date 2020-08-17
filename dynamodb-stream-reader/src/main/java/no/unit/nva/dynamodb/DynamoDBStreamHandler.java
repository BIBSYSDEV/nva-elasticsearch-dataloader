package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Publication;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);
    private final ObjectMapper objectMapper = JsonUtils.objectMapper;


    private final Function<DynamodbEvent.DynamodbStreamRecord, String> getIdentifier =
            (DynamodbEvent.DynamodbStreamRecord streamRecord) -> {
        Map<String, AttributeValue> keys = streamRecord.getDynamodb().getKeys();
        AttributeValue identifierattribute = keys.get("identifier");
        String identifier = identifierattribute.getS();
        logger.debug("extracted identifier: {}", identifier);
        return identifier;
    };

    private final Function<String, Publication> getPublication = (String identifier) -> {
        Publication publication = new Publication();
        logger.debug("retrieved publication: {}", publication);
        return publication;
    };

    private final Function<Publication, Publication> flattenPublication = (Publication publication)  -> {
        Publication flattenedPublication = publication;
        logger.debug("flattened publication: {}", publication);
        return flattenedPublication;
    };

    private final Consumer<Publication> updateSearchIndex = (Publication publication) -> {
        // use search API to upload flattened publication into ElasticSearch
        logger.debug("Updated search API with publication: {}", publication);
    };


    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {

    }



    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        try {
            logger.debug("logged event: " + objectMapper.writeValueAsString(event));
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

        String identifier  = getIdentifier.apply(streamRecord);
        Publication publication = getPublication.apply(identifier);
        Publication flattenedPublication = flattenPublication.apply(publication);
        updateSearchIndex.accept(flattenedPublication);
        logger.debug("Upserting search index for identifier  " + identifier);

    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        String identifier  = getIdentifier.apply(streamRecord);
        logger.debug("Deleting from search API publication with identifier: {}", identifier);
    }


}
