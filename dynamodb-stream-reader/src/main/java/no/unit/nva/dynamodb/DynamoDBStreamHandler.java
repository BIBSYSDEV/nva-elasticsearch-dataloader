package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
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

    private final Function<DynamodbEvent.DynamodbStreamRecord, Map<String, AttributeValue>> getPublicationImageFromStream =
            (DynamodbEvent.DynamodbStreamRecord streamRecord) -> {
                Map<String, AttributeValue> newImage = streamRecord.getDynamodb().getNewImage();
                return newImage;

            };

    private final Function<String, Publication> getPublication = (String identifier) -> {
        Publication publication = new Publication();
        logger.debug("retrieved publication: {}", publication);
        return publication;
    };

    private final Function<Map<String, AttributeValue>, FlattenedPublicationIndexRecord> flattenImageToIndexRecord = (Map<String, AttributeValue> publicationImage) -> {
        FlattenedPublicationIndexRecord flattenedPublication = new FlattenedPublicationIndexRecord(publicationImage.get("identifier").getS());
        logger.debug("flattened publication: {}", flattenedPublication);
        publicationImage.forEach((k, v) -> {
            if (v.getM() == null) {
                flattenedPublication.putIndexValue(k, v.getS());
            } else {

            }
        });
        return flattenedPublication;
    };


    private void flatten(FlattenedPublicationIndexRecord target, String prefix, Map<String, AttributeValue> valueMap) {
        logger.debug("flatten: {}", valueMap);
        valueMap.forEach((k, v) -> {
            if (v.getM() == null) {
                target.putIndexValue(addIndexPrefix(prefix,k), v.getS());
            } else {
                flatten(target,k, v.getM());
            }
        });
    }

    private String addIndexPrefix(String prefix, String k) {
        if (prefix != null && !prefix.isEmpty()) {
            return prefix+"."+k;
        } else {
            return k;
        }
    }

    ;



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
//        try {
//            logger.debug("logged event: " + objectMapper.writeValueAsString(event));
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }

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

        String identifier = getIdentifier.apply(streamRecord);
//        Publication publication = getPublication.apply(identifier);

        Map<String, AttributeValue> valueMap = getPublicationImageFromStream.apply(streamRecord);
        FlattenedPublicationIndexRecord flattenedPublication = new FlattenedPublicationIndexRecord(identifier);
        flatten(flattenedPublication, "", valueMap);
//        FlattenedPublicationIndexRecord flattenedPublication = flattenImageToIndexRecord.apply(valueMap);
//        updateSearchIndex.accept(flattenedPublication);
            logger.debug("Upserting search index for identifier {} with values {}",identifier, flattenedPublication);
    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        String identifier = getIdentifier.apply(streamRecord);
        logger.debug("Deleting from search API publication with identifier: {}", identifier);
    }


}
