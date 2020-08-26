package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import nva.commons.utils.IoUtils;
import nva.commons.utils.JsonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

public class ResourceUtils {

    /**
     * Creates a DynamoDB event from a file.
     *
     * @param filename file to read json source from
     * @return event created from file
     * @throws IOException when something goes wrong
     */
    public static DynamodbEvent loadEventFromResourceFile(String filename) throws IOException {
        InputStream is = IoUtils.inputStreamFromResources(Paths.get(filename));
        return JsonUtils.objectMapper.readValue(is, DynamodbEvent.class);
    }

}
