package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import elasticsearch.ElasticSearchRestClient;
import nva.commons.utils.JacocoGenerated;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    public static final String IDENTIFIER = "identifier";

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);
    public static final String DATE_YEAR = "date.year";
    public static final String DESCRIPTION_MAIN_TITLE = "entityDescription.mainTitle";
    public static final String CONTRIBUTORS_IDENTIITY_NAME = "entityDescription.contributors.identiity.name";
    public static final String PUBLICATION_TYPE = "type";
    public static final String YEAR = "year";
    public static final String TITLE = "title";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    private final ElasticSearchRestClient elasticSearchClient = new ElasticSearchRestClient();
    private final UnaryOperator<String> indexMapping;
    private final Predicate<String> indexfilter;

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {
        indexMapping = new IndexMapperBuilder()
                .withIndex(DATE_YEAR, YEAR)
                .withIndex(DESCRIPTION_MAIN_TITLE, TITLE)
                .withIndex(CONTRIBUTORS_IDENTIITY_NAME, NAME)
                .withIndex(PUBLICATION_TYPE, TYPE)
                .build();

        indexfilter = new IndexFilterBuilder()
                .withIndex(DATE_YEAR)
                .withIndex(DESCRIPTION_MAIN_TITLE)
                .withIndex(CONTRIBUTORS_IDENTIITY_NAME)
                .withIndex(PUBLICATION_TYPE)
                .build();

    }

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        try {
            logger.debug("event={}", JsonUtils.objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
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
        logger.debug("valueMap={}", valueMap.toString());
        ValueMapFlattener flattener = new ValueMapFlattener.Builder()
                .withIndexFilter(indexfilter)
                .withIndexMapping(indexMapping)
                .withSeparator(".")
                .build();
        PublicationIndexDocument flattenedPublication = flattener.flattenValueMap(identifier, valueMap);
        logger.debug("Upserting search index for identifier {} with values {}",identifier, flattenedPublication);
        elasticSearchClient.addDocumentToIndex(flattenedPublication);
    }

    private void removeFromSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord) {
        String identifier = streamRecord.getDynamodb().getKeys().get(IDENTIFIER).getS();
        logger.trace("Deleting from search API publication with identifier: {}", identifier);
        elasticSearchClient.removeDocumentFromIndex(identifier);
    }




}
