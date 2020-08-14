package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Publication;
import nva.commons.utils.JacocoGenerated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

        private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);

    /**
     * Default constructor for DynamoDBStreamHandler.
     */
    @JacocoGenerated
    public DynamoDBStreamHandler() {

    }

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        try {
            System.out.println("DynamodbEvent: "+ new ObjectMapper().writeValueAsString(event));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        for (DynamodbEvent.DynamodbStreamRecord rec: event.getRecords()) {
            try {
                Publication publication = new NvaResourceExtractor().extractPublication(rec);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        return "Handled " + event.getRecords().size() + " records";
    }



}
