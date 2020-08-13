package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import nva.commons.utils.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DynamoDBStreamHandler implements RequestHandler<Map<String,Object>, Map<String,Object>> {

        private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {

    }

    @Override
    public Map<String,Object> handleRequest(Map<String,Object> event, Context context) {
        System.out.println("event: "+ event);
        logger.debug("event: {}",event);
        return event;
    }

}
