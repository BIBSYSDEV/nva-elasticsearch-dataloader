package no.unit.nva.dynamodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IndexFilterBuilderTest {


    public static final String INDEX = "tulliball";

    @Test
    @DisplayName("Testing building and function of Predicate<String>  to do IndexFiltering")
    public void testIndexFilterWithIndex() {
        Predicate<String> predicate = new IndexFilterBuilder().withIndex(INDEX).build();
        assertNotNull(predicate);
        assertTrue(predicate.test(INDEX));
    }

    @Test
    @DisplayName("Testing building and function of Predicate<String>  to allow all indexes")
    public void testIndexFilterAllowAll() {
        Predicate<String> predicate = new IndexFilterBuilder().doAllowAll().build();
        assertNotNull(predicate);
        assertTrue(predicate.test(INDEX));
    }

    @Test
    @DisplayName("Testing building and function of Predicate<String> to block unknown index")
    public void testIndexFilterWithoutIndex() {
        Predicate<String> predicate = new IndexFilterBuilder().build();
        assertNotNull(predicate);
        assertFalse(predicate.test(INDEX));
    }


}
