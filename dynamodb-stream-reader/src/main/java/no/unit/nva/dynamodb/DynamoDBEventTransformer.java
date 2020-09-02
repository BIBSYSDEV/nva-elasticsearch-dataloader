package no.unit.nva.dynamodb;

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import no.unit.nva.elasticsearch.Constants;
import no.unit.nva.elasticsearch.ElasticSearchIndexDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static no.unit.nva.elasticsearch.Constants.CONTRIBUTORS_IDENTITY_NAME;
import static no.unit.nva.elasticsearch.Constants.CREATED_DATE_KEY;
import static no.unit.nva.elasticsearch.Constants.DATE_YEAR;
import static no.unit.nva.elasticsearch.Constants.DESCRIPTION_MAIN_TITLE;
import static no.unit.nva.elasticsearch.Constants.MODIFIED_DATE_KEY;
import static no.unit.nva.elasticsearch.Constants.OWNER_NAME_KEY;
import static no.unit.nva.elasticsearch.Constants.PUBLICATION_TYPE;

public class DynamoDBEventTransformer {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBEventTransformer.class);

    public static final String UNKNOWN_VALUE_KEY_MESSAGE = "Unknown valueKey: {}";


    private final Set<String> wantedIndexes = Set.of(DATE_YEAR,
            DESCRIPTION_MAIN_TITLE,
            CONTRIBUTORS_IDENTITY_NAME,
            PUBLICATION_TYPE);

    /**
     * Creates a DynamoDBEventTransformer which creates a ElasticSearchIndexDocument from an dynamoDBEvent.
     */
    public DynamoDBEventTransformer() {
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
        parse(document, Constants.EMPTY_STRING, valueMap);
        return document;
    }

    private void parse(ElasticSearchIndexDocument document, String prefix, Map<String, AttributeValue> valueMap) {
        valueMap.forEach((k, v) -> {
            if (v != null) {
                String key = addIndexPrefix(prefix, k);
                if (isASimpleValue(v)) {
                    if (wantedIndexes.contains(key)) {
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
                            if (isASimpleValue(attributeValue)) {
                                if (wantedIndexes.contains(key)) {
                                    assignValueToIndexDocument(document, key, v.getS());
                                }
                            } else {
                                parse(document, key, attributeValue.getM());
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
            case OWNER_NAME_KEY:
                document.setOwner(value);
                break;
            case CONTRIBUTORS_IDENTITY_NAME:
                document.addContributorName(value);
                break;
            case DESCRIPTION_MAIN_TITLE:
                document.setTitle(value);
                break;
            case CREATED_DATE_KEY:
                document.setCreatedDate(value);
                break;
            case MODIFIED_DATE_KEY:
                document.setModifiedDate(value);
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
            return prefix + Constants.SIMPLE_DOT_SEPARATOR + k;
        } else {
            return k;
        }
    }

}
