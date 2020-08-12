package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent event, Context context> {


    private static final Logger logger = LoggerFactory.getLogger(PostAuthenticationHandler.class);

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {
        this(
            new CustomerApiClient(HttpClient.newHttpClient(), new ObjectMapper(), new Environment()),
            AWSCognitoIdentityProviderClient.builder().build()
        );

    }

    @Override
    public Map<String,Object> handleRequest(DynamodbEvent event, Context context) {
        return event;
    }

}
