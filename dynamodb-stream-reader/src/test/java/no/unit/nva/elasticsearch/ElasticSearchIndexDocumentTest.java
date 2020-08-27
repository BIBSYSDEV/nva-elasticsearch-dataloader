package no.unit.nva.elasticsearch;

import elasticsearch.ElasticSearchIndexDocument;
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
    public static final String SAMPLE_DATE = "2020-08-26";
    public static final String SAMPLE_RESOURCETYPE = "sampleResourceType";
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


        document.setResourceType(SAMPLE_RESOURCETYPE);
        document.setDate(SAMPLE_DATE);
        document.addContributorName(SAMPLE_CONTRIBUTOR);
        document.addContributorName(SAMPLE_CONTRIBUTOR2);
        document.setTitle(SAMPLE_TITLE);

        String json = document.toJson();

        assertTrue(json.contains(ElasticSearchIndexDocument.ID_KEY));
        assertTrue(json.contains(SAMPLE_DATE));
        assertTrue(json.contains(SAMPLE_RESOURCETYPE));
        assertTrue(json.contains(SAMPLE_CONTRIBUTOR));
        assertTrue(json.contains(SAMPLE_CONTRIBUTOR2));
        assertTrue(json.contains(SAMPLE_TITLE));

    }

}
