package no.unit.nva.dynamodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IndexFilterBuilderTest {


    public static final String INDEX = "tulliball";

    @Test
    @DisplayName("Testing building of Indexfiler wuth indexed")
    public void testIndexFilterWithIndex() {
        Predicate<String> predicate = new IndexFilterBuilder().withIndex(INDEX).build();
        assertNotNull(predicate);
        assertTrue(predicate.test(INDEX));
    }

}
