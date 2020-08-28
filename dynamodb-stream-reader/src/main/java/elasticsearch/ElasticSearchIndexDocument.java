package elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import nva.commons.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchIndexDocument {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchIndexDocument.class);

    private static final String CONTRIBUTORS_KEY = "contributor";
    private static final String TITLE_KEY = "title";
    private static final String DATE_KEY = "date";
    private static final String RESOURCE_TYPE_KEY = "type";
    public static final String ID_KEY = "id";
    public static final String EMPTY_STRING = "";
    private static final String NAME_KEY = "name";

    private final String internalIdentifier;

    private final String serviceUrl;
    private final String indexName;
    private final Map<String, Object> values = new HashMap<>();

    /**
     * Created an document suitable to upload to elasticsearch.
     * @param indexName which elasticsearch index handles this document
     * @param serviceUrl prefix of serviceurl for creating URI for resource source
     * @param internalIdentifier shor internal identifier (uuid) for resource
     */
    public ElasticSearchIndexDocument(String indexName, String serviceUrl,  String internalIdentifier) {
        this.indexName = indexName;
        this.serviceUrl = serviceUrl;
        this.internalIdentifier = internalIdentifier;
        values.put(ID_KEY, serviceUrl + internalIdentifier);
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getInternalIdentifier() {
        return internalIdentifier;
    }

    /**
     * Adds a contributors name to the document.
     * The document an hold multiple contributors
     * @param name name of the contributor
     */
    public void addContributorName(String name) {
        List<Object> contributors =
                (List<Object>) values.computeIfAbsent(CONTRIBUTORS_KEY, l ->  new ArrayList<Object>());

        Map<String, String> contributorMap = new HashMap<>();
        contributorMap.put(NAME_KEY,name);
        contributors.add(contributorMap);
        values.put(CONTRIBUTORS_KEY,contributors);
    }

    /**
     * Sets the title of the resource in document.
     * @param title title of resource
     */
    public void setTitle(String title) {
        values.put(TITLE_KEY, title);
    }

    /**
     * Date assosiated with the resource.
     * @param dateString date on the form "YYYY-MM-DD"
     */
    public void setDate(String dateString) {
        values.put(DATE_KEY, dateString);
    }

    /**
     * Sets the type of the resource.
     * @param resourceType type of resource. ie. JournalArticle
     */
    public void setResourceType(String resourceType) {
        values.put(RESOURCE_TYPE_KEY, resourceType);
    }

    /**
     * Generates JSON string to be inserted into elasticsearch.
     * @return string representation of the document
     */
    public String toJson() {
        try {
            return JsonUtils.objectMapper.writeValueAsString(values);
        } catch (JsonProcessingException e) {
            logger.error(EMPTY_STRING,e);
            throw new RuntimeException(e);
        }
    }


}
