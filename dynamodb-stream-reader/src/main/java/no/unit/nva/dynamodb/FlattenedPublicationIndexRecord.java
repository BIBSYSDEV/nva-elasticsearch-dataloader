package no.unit.nva.dynamodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FlattenedPublicationIndexRecord {

    private final String identifier;
    private final Map<String, String> indexValues = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);


    public FlattenedPublicationIndexRecord(String identifier) {
        this.identifier = identifier;
    }

    public String putIndexValue(String index, String value) {
        if (indexValues.containsKey(index)) {
            logger.warn("Index {} already exists with value {}, new values {}", index, indexValues.get(index)  ,value);
        }
        return indexValues.put(index, value);
    }

    @Override
    public String toString() {
        return "FlattenedPublicationIndexRecord{" +
                "identifier='" + identifier + '\'' +
                ", indexValues=" + indexValues +
                '}';
    }
}
