package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

public class ResourceUtils {

    /**
     * Creates a DynamoDB event from a file.
     *
     * @param filename file to read json source from
     * @return event created from file
     */
    public static DynamodbEvent loadEventFromResourceFile(String filename) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(filename);
        ObjectMapper mapper = new ObjectMapper();
        DynamodbEvent event = null;
        try {
            event = mapper.readValue(is, DynamodbEvent.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return event;
    }

}
