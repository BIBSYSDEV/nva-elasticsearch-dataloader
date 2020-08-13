package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import nva.commons.utils.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, Map<String,Object>> {

        private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {

    }

    @Override
    public Map<String,Object> handleRequest(DynamodbEvent event, Context context) {
        System.out.println("DynamodbEvent: "+ event);
        logger.debug("event: {}",event);
        return Map.of("event", event);
    }

}
