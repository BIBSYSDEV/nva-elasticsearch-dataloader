package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import elasticsearch.ElasticSearchRestClient;
import nva.commons.utils.Environment;
import nva.commons.utils.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);

    public static final String IDENTIFIER = "identifier";
    public static final String DATE_YEAR = "entityDescription.date.year";
    public static final String DESCRIPTION_MAIN_TITLE = "entityDescription.mainTitle";
    public static final String CONTRIBUTORS_IDENTITY_NAME = "entityDescription.contributors.identity.name";
    public static final String PUBLICATION_TYPE = "publicationInstance.type";
    public static final String YEAR = "year";
    public static final String TITLE = "title";
    public static final String NAME = "author";
    public static final String TYPE = "publicationType";
    private final ElasticSearchRestClient elasticSearchClient;

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {
        this(new ElasticSearchRestClient(HttpClient.newHttpClient(), new Environment()));
    }

    /**
     * Constructor for DynamoDBStreamHandler for testing.
     * @param elasticSearchRestClient elasticSearchRestClient to be injected for testing
     */
    @JacocoGenerated
    public DynamoDBStreamHandler(ElasticSearchRestClient elasticSearchRestClient) {
        this.elasticSearchClient = elasticSearchRestClient;
    }



    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        for (DynamodbEvent.DynamodbStreamRecord streamRecord : event.getRecords()) {
            switch (streamRecord.getEventName()) {
                case "INSERT":
                case "MODIFY":
                    try {
                        upsertSearchIndex(streamRecord);
                    } catch (InterruptedException | URISyntaxException | IOException e) {
                        logger.error("Error Upserting index", e);
                    }
                    break;
                case "REMOVE":
                    try {
                        removeFromSearchIndex(streamRecord);
                    } catch (InterruptedException | URISyntaxException | IOException e) {
                        logger.error("Error deleting from index", e);
                    }
                    break;
                default:
                    throw new RuntimeException("Not a known operation");
            }
        }
        return "Handled " + event.getRecords().size() + " records";
    }

    private void upsertSearchIndex(DynamodbEvent.DynamodbStreamRecord streamRecord)
            throws InterruptedException, IOException, URISyntaxException {
        String identifier = getIdentifierFromStreamRecord(streamRecord);
        Map<String, AttributeValue> valueMap = streamRecord.getDynamodb().getNewImage();
        logger.trace("valueMap={}", valueMap.toString());

        Predicate<String> indexfilter = new IndexFilterBuilder()
                .withIndex(DATE_YEAR)
                .withIndex(DESCRIPTION_MAIN_TITLE)
                .withIndex(CONTRIBUTORS_IDENTITY_NAME)
                .withIndex(PUBLICATION_TYPE)
                .build();

        UnaryOperator<String> indexMapping = new IndexMapperBuilder()
                .withIndex(DATE_YEAR, YEAR)
                .withIndex(DESCRIPTION_MAIN_TITLE, TITLE)
                .withIndex(CONTRIBUTORS_IDENTITY_NAME, NAME)
                .withIndex(PUBLICATION_TYPE, TYPE)
                .build();

        ValueMapFlattener flattener = new ValueMapFlattener.Builder()
                .withIndexFilter(indexfilter)
                .withIndexMapping(indexMapping)
                .withSeparator(".")
                .build();

        Map<String, String> flattenedPublication = flattener.flattenValueMap(identifier, valueMap);
        elasticSearchClient.addDocumentToIndex(flattenedPublication);

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
