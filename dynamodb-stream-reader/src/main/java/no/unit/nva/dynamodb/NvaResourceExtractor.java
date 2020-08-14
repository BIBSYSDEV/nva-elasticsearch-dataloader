package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.unit.nva.model.Publication;


public class NvaResourceExtractor {

    /**
     * Extracts Publication from DynamoDB event.
     *
     * @param dynamoDbSource source of record in event
     * @return Changed Publication (NVA resource) from DynamoDB event
     * @throws JsonProcessingException  when something goes wrong
     */
    public Publication extractPublication(DynamodbEvent.DynamodbStreamRecord dynamoDbSource)
            throws JsonProcessingException {
        String eventSource = dynamoDbSource.getEventSource();
        System.out.println("eventSource: " + eventSource);
        Publication publication =  new ObjectMapper().readValue(eventSource, Publication.class);
        return publication;
    }
}
