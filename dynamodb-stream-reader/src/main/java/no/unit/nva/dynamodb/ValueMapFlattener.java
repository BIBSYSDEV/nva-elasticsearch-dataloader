package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ValueMapFlattener {

    private static final Logger logger = LoggerFactory.getLogger(ValueMapFlattener.class);
    private static final String IDENTIFIER = "identifier";

    private final String separator;

    public static class Builder {

        private ValueMapFlattener valueMapFlattener;
        private String separator;

        public Builder withSeparator(String separator){
            this.separator = separator;
            return this;  //By returning the builder each time, we can create a fluent interface.
        }

        public ValueMapFlattener build() {
            valueMapFlattener = new ValueMapFlattener(separator);
            return valueMapFlattener;
        }
    }

    private ValueMapFlattener(String separator) {
        this.separator = separator;
    }

    public PublicationIndexDocument flattenValueMap(Map<String, AttributeValue> valueMap) {
        PublicationIndexDocument flattenedPublication = new PublicationIndexDocument(valueMap.get(IDENTIFIER).getS());
        flatten(flattenedPublication, "", valueMap);
        return  flattenedPublication;
    }

    private void flatten(PublicationIndexDocument target, String prefix, Map<String, AttributeValue> valueMap) {
        logger.debug("flatten: {}", valueMap);
        valueMap.forEach((k, v) -> {
            if (v.getM() == null) {
                target.putIndexValue(addIndexPrefix(prefix,k), v.getS());
            } else {
                flatten(target,k, v.getM());
            }
        });
    }

    private String addIndexPrefix(String prefix, String k) {
        if (prefix != null && !prefix.isEmpty()) {
            return prefix + separator + k;
        } else {
            return k;
        }
    };
}
