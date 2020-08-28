package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import elasticsearch.ElasticSearchIndexDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class DynamoDBEventTransformer {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBEventTransformer.class);

    public static final String EMPTY_STRING = "";
    public static final String DATE_YEAR = "entityDescription.date.year";
    public static final String DESCRIPTION_MAIN_TITLE = "entityDescription.mainTitle";
    public static final String CONTRIBUTORS_IDENTITY_NAME = "entityDescription.contributors.identity.name";
    public static final String PUBLICATION_TYPE = "publicationInstance.type";
    public static final String UNKNOWN_VALUE_KEY_MESSAGE = "Unknown valueKey: {}";

    private final String separator;
    private final Predicate<String> indexFilter;

    public static class Builder {

        private DynamoDBEventTransformer transformer;
        private String separator = EMPTY_STRING;
        private Predicate<String> indexFilter = new IndexFilterBuilder().doAllowAll().build();

        public Builder withSeparator(String separator) {
            this.separator = separator;
            return this;
        }

        public Builder withIndexFilter(Predicate<String> filter) {
            this.indexFilter = filter;
            return this;
        }

        public DynamoDBEventTransformer build() {
            transformer = new DynamoDBEventTransformer(separator, indexFilter);
            return transformer;
        }
    }

    private DynamoDBEventTransformer(String separator,
                                     Predicate<String> indexFilter) {
        this.separator = separator;
        this.indexFilter = indexFilter;
    }

    /**
     * Transforms a nested valuemap read from DynamoDB streamrecord into ElasticSearchIndexDocument.
     * @param identifier of the original dynamoDB record
     * @param valueMap Map containing the values associated with the record
     * @return A document usable for indexing in elasticsearch
     */
    public ElasticSearchIndexDocument parseValueMap(String elasticSearchIndexName,
                                                    String targetServiceUrl,
                                                    String identifier,
                                                    Map<String, AttributeValue> valueMap) {
        ElasticSearchIndexDocument document =
                new ElasticSearchIndexDocument(elasticSearchIndexName, targetServiceUrl, identifier);
        parse(document, "", valueMap);
        return document;
    }

    private void parse(ElasticSearchIndexDocument document, String prefix, Map<String, AttributeValue> valueMap) {
        logger.trace("flatten: prefix={} values={}", prefix, valueMap);
        valueMap.forEach((k, v) -> {
            if (v != null) {
                String key = addIndexPrefix(prefix, k);
                if (isASimpleValue(v)) {
                    if (indexFilter.test(key)) {
                        assignValueToIndexDocument(document,key, v.getS());
                    }
                } else {
                    if (v.getL() == null) {
                        // This must be a map element
                        parse(document, key, v.getM());
                    } else {
                        // This must be a list/JSON-array
                        List<AttributeValue> listElements = v.getL();
                        for (AttributeValue attributeValue : listElements) {
                            String elementKey = key; // + "-" + i;
                            if (isASimpleValue(attributeValue)) {
                                if (indexFilter.test(elementKey)) {
                                    assignValueToIndexDocument(document, key, v.getS());
                                }
                            } else {
                                parse(document, elementKey, attributeValue.getM());
                            }
                        }
                    }
                }
            }
        });
    }



    private void assignValueToIndexDocument(ElasticSearchIndexDocument document, String valueKey, String value) {
        switch (valueKey) {
            case PUBLICATION_TYPE:
                document.setResourceType(value);
                break;
            case CONTRIBUTORS_IDENTITY_NAME:
                document.addContributorName(value);
                break;
            case DESCRIPTION_MAIN_TITLE:
                document.setTitle(value);
                break;
            case DATE_YEAR:
                document.setDate(value);
                break;
            default:
                logger.debug(UNKNOWN_VALUE_KEY_MESSAGE, valueKey);
                break;
        }
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
