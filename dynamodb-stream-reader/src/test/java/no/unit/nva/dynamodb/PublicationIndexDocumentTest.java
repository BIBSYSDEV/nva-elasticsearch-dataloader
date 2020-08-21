package no.unit.nva.dynamodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PublicationIndexDocumentTest {


    public static final String IDENTIFIER = "tulliball";

    @Test
    @DisplayName("Testing creation of PublicationIndexDocument")
    public void testCreatingPublicationIndexDocument() {
        PublicationIndexDocument publicationIndexDocument = new PublicationIndexDocument(IDENTIFIER);
        assertNotNull(publicationIndexDocument);
        assertNotNull(publicationIndexDocument.toString());
        assertEquals(IDENTIFIER, publicationIndexDocument.getIdentifier());

    }

}
