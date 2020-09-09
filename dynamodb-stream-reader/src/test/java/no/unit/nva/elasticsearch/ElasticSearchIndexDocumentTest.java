package no.unit.nva.elasticsearch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElasticSearchIndexDocumentTest {

    private static final String INDEX_NAME = "someIndex";
    private static final String SERVICE_URL = "http://localhost/";
    private static final String INTERNAL_IDENTIFIER = "id";
    public static final String SAMPLE_CONTRIBUTOR = "Doe, John";
    public static final String SAMPLE_CONTRIBUTOR2 = "Doe, Jane";
    public static final String SAMPLE_CONTRIBUTOR3 = "Andrè, Noen";
    public static final String SAMPLE_OWNER = "na@unit.no";
    public static final String SAMPLE_DATE = "2020-08-26";
    public static final String SAMPLE_CREATED_TIMESTAMP = "2020-08-20T11:58:40.390961Z";
    public static final String SAMPLE_MODIFIED_TIMESTAMP = "2020-08-26T12:58:40.390961Z";
    public static final String SAMPLE_RESOURCE_TYPE = "sampleResourceType";
    public static final String SAMPLE_TITLE = "This Is A Sample Title";

    @Test
    @DisplayName("Testing ElasticSearchIndexDocument constructor and initializing")
    public void testElasticSearchIndexDocumentConstructor() {
        ElasticSearchIndexDocument document =
                new ElasticSearchIndexDocument(INDEX_NAME, SERVICE_URL, INTERNAL_IDENTIFIER);
        assertNotNull(document);
        assertEquals(INDEX_NAME, document.getIndexName());
        assertEquals(SERVICE_URL, document.getServiceUrl());
        assertEquals(INTERNAL_IDENTIFIER, document.getInternalIdentifier());
        assertTrue(document.toJson().contains(ElasticSearchIndexDocument.ID_KEY));
    }

    @Test
    @DisplayName("Testing ElasticSearchIndexDocument constructor, minimum values added")
    public void testElasticSearchIndexDocumentConstructorAndCompleteAssignment() {
        ElasticSearchIndexDocument document =
                new ElasticSearchIndexDocument(INDEX_NAME, SERVICE_URL, INTERNAL_IDENTIFIER);
        assertNotNull(document);


        document.setResourceType(SAMPLE_RESOURCE_TYPE);
        document.setDate(SAMPLE_DATE);
        document.addContributorName(SAMPLE_CONTRIBUTOR);
        document.addContributorName(SAMPLE_CONTRIBUTOR2);
        document.addContributorName(SAMPLE_CONTRIBUTOR3);
        document.setTitle(SAMPLE_TITLE);

        document.setModifiedDate(SAMPLE_MODIFIED_TIMESTAMP);
        document.setCreatedDate(SAMPLE_CREATED_TIMESTAMP);
        document.setOwner(SAMPLE_OWNER);

        String json = document.toJson();

        assertTrue(json.contains(ElasticSearchIndexDocument.ID_KEY));
        assertTrue(json.contains(SAMPLE_DATE));
        assertTrue(json.contains(SAMPLE_RESOURCE_TYPE));
        assertTrue(json.contains(SAMPLE_CONTRIBUTOR));
        assertTrue(json.contains(SAMPLE_CONTRIBUTOR2));
        assertTrue(json.contains(SAMPLE_CONTRIBUTOR3));
        assertTrue(json.contains(SAMPLE_TITLE));

        assertTrue(json.contains(SAMPLE_OWNER));
        assertTrue(json.contains(SAMPLE_CREATED_TIMESTAMP));
        assertTrue(json.contains(SAMPLE_MODIFIED_TIMESTAMP));



    }

}
