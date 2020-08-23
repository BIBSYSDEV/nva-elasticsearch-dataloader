package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class ValueMapFlattener {

    private static final Logger logger = LoggerFactory.getLogger(ValueMapFlattener.class);

    private final String separator;
    private final Predicate<String> indexFilter;
    private final UnaryOperator<String> indexMapper;

    public static class Builder {

        private ValueMapFlattener valueMapFlattener;
        private String separator = "";
        private Predicate<String> indexFilter = new IndexFilterBuilder().doAllowAll().build();
        private UnaryOperator<String> indexMapping;

        public Builder withSeparator(String separator) {
            this.separator = separator;
            return this;  //By returning the builder each time, we can create a fluent interface.
        }

        public Builder withIndexFilter(Predicate<String> filter) {
            this.indexFilter = filter;
            return this;  //By returning the builder each time, we can create a fluent interface.
        }

        public Builder withIndexMapping(UnaryOperator<String> mapping) {
            this.indexMapping = mapping;
            return this;
        }

        public ValueMapFlattener build() {
            valueMapFlattener = new ValueMapFlattener(separator, indexFilter, indexMapping);
            return valueMapFlattener;
        }
    }

    private ValueMapFlattener(String separator, Predicate<String> indexFilter, UnaryOperator<String> indexMapper) {
        this.separator = separator;
        this.indexFilter = indexFilter;
        this.indexMapper = indexMapper;
    }

    /**
     * Flattens a nested valuemap read from DynamoDB streamrecord.
     * @param identifier of the original dynamoDB record
     * @param valueMap Map containing the values associated with the record
     * @return A document usable for indexing in elasticsearch
     */
    public PublicationIndexDocument flattenValueMap(String identifier, Map<String, AttributeValue> valueMap) {
        PublicationIndexDocument flattenedPublication = new PublicationIndexDocument(identifier);
        flatten(flattenedPublication, "", valueMap);
        return flattenedPublication;
    }

    private void flatten(PublicationIndexDocument target, String prefix, Map<String, AttributeValue> valueMap) {
        logger.trace("flatten: prefix={} values={}", prefix, valueMap);
        valueMap.forEach((k, v) -> {
            if (v != null) {
                String key = addIndexPrefix(prefix, k);
                if (isASimpleValue(v)) {
                    if (indexFilter.test(key)) {
                        target.putIndexValue(indexMapper.apply(key), v.getS());
                    }
                } else {
                    if (v.getL() == null) {
                        // This must be a map element
                        flatten(target, key, v.getM());
                    } else {
                        // This must be a list/JSON-array
                        List<AttributeValue> listElements = v.getL();
                        for (AttributeValue attributeValue : listElements) {
                            String elementKey = key; // + "-" + i;
                            if (isASimpleValue(attributeValue)) {
                                if (indexFilter.test(elementKey)) {
                                    target.putIndexValue(indexMapper.apply(elementKey), attributeValue.getS());
                                }
                            } else {
                                flatten(target, elementKey, attributeValue.getM());
                            }
                        }
                    }
                }
            }
        });
    }


    private boolean isASimpleValue(AttributeValue attributeValue) {
        return attributeValue.getM() == null && attributeValue.getL() == null;
    }

    private String addIndexPrefix(String prefix, String k) {
        if (prefix != null && !prefix.isEmpty()) {
            return prefix + separator + k;
        } else {
            return k;
        }
    }

}
