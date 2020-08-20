package no.unit.nva.dynamodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class IndexMapperBuilderTest {


    public static final String FROMINDEX = "index1";
    public static final String TOINDEX = "index2";

    @Test
    @DisplayName("Testing mappping of Indexfiler wuth indexed")
    public void testIndexFilterWithIndex() {
        UnaryOperator<String> operator = new IndexMapperBuilder().withIndex(FROMINDEX, TOINDEX).build();
        assertNotNull(operator);
        assertEquals(TOINDEX, operator.apply(FROMINDEX));
    }

}
