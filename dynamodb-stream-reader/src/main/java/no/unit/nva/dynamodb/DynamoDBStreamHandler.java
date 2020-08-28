package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import elasticsearch.ElasticSearchIndexDocument;
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

    public static final String ELASTICSEARCH_ENDPOINT_INDEX_KEY = "ELASTICSEARCH_ENDPOINT_INDEX";

    public static final String IDENTIFIER = "identifier";
    public static final String DATE_YEAR = "entityDescription.date.year";
    public static final String DESCRIPTION_MAIN_TITLE = "entityDescription.mainTitle";
    public static final String CONTRIBUTORS_IDENTITY_NAME = "entityDescription.contributors.identity.name";
    public static final String PUBLICATION_TYPE = "publicationInstance.type";
    public static final String YEAR = "year";
    public static final String TITLE = "title";
    public static final String NAME = "author";
    public static final String TYPE = "publicationType";
    public static final String ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE = "Error processing DynamoDBEvent";
    public static final String SUCCESS_MESSAGE = "200 OK";
    public static final String TARGET_SERVICE_URL_KEY = "TARGET_SERVICE_URL_KEY";
    private final ElasticSearchRestClient elasticSearchClient;
    private String targetServiceUrl;
    private String elasticSearchEndpointIndex;

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
        try {
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
        } catch (InterruptedException | URISyntaxException | IOException e) {
            logger.error(ERROR_PROCESSING_DYNAMO_DBEVENT_MESSAGE, e);
            throw new RuntimeException(e);
        }

        return SUCCESS_MESSAGE;
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

        DynamoDBEventTransformer eventTransformer = new DynamoDBEventTransformer.Builder()
                .withIndexFilter(indexfilter)
                .withIndexMapping(indexMapping)
                .withSeparator(".")
                .build();

        ElasticSearchIndexDocument document = eventTransformer.parseValueMap(elasticSearchEndpointIndex, targetServiceUrl, identifier, valueMap);
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
