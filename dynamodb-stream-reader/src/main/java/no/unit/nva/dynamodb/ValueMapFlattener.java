package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class ValueMapFlattener {

    private static final Logger logger = LoggerFactory.getLogger(ValueMapFlattener.class);

    private final String separator;
    private final Predicate<String> indexFilter;

    public static class Builder {

        private ValueMapFlattener valueMapFlattener;
        private String separator = "";
        private Predicate<String> indexFilter = new IndexFilterBuilder().doAllowAll().build();
        private UnaryOperator<String> indexMapping;

        public Builder withSeparator(String separator){
            this.separator = separator;
            return this;  //By returning the builder each time, we can create a fluent interface.
        }

        public Builder withIndexFilter(Predicate<String> filter){
            this.indexFilter = filter;
            return this;  //By returning the builder each time, we can create a fluent interface.
        }

        public Builder withIndexMapping(UnaryOperator<String> mapping){
            this.indexMapping = mapping;
            return this;  //By returning the builder each time, we can create a fluent interface.
        }


        public ValueMapFlattener build() {
            valueMapFlattener = new ValueMapFlattener(separator, indexFilter);
            return valueMapFlattener;
        }
    }

    private ValueMapFlattener(String separator, Predicate<String> indexFilter) {
        this.separator = separator;
        this.indexFilter = indexFilter;
    }

    public PublicationIndexDocument flattenValueMap(String identifier, Map<String, AttributeValue> valueMap) {
        PublicationIndexDocument flattenedPublication = new PublicationIndexDocument(identifier);
        flatten(flattenedPublication, "", valueMap);
        return  flattenedPublication;
    }

    private void flatten(PublicationIndexDocument target, String prefix, Map<String, AttributeValue> valueMap) {
        logger.trace("flatten: {}", valueMap);
        valueMap.forEach((k, v) -> {
            if (v.getM() == null) {
                String key = addIndexPrefix(prefix, k);
                if (indexFilter.test(key)) {
                    target.putIndexValue(key, v.getS());
                }
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
