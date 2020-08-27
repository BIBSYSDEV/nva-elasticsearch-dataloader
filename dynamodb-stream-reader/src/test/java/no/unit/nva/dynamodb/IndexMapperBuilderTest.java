package no.unit.nva.dynamodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class IndexMapperBuilderTest {


    public static final String FROMINDEX = "index1";
    public static final String TOINDEX = "index2";
    public static final String DUMMYINDEX = "dummy";

    @Test
    @DisplayName("Testing IndexMapperBuilder creating Operator with simple index mapping")
    public void testIndexFilterWithIndex() {
        UnaryOperator<String> operator = new IndexMapperBuilder().withIndex(FROMINDEX, TOINDEX).build();
        assertNotNull(operator);
        assertEquals(TOINDEX, operator.apply(FROMINDEX));
    }

    @Test
    @DisplayName("Testing IndexMapperBuilder creating Operator with passthru behavior")
    public void testIndexFilterWithPassthru() {
        UnaryOperator<String> operator = new IndexMapperBuilder().doPassThrough().build();
        assertNotNull(operator);
        assertEquals(DUMMYINDEX, operator.apply(DUMMYINDEX));
    }

    @Test
    @DisplayName("Testing IndexMapperBuilder creates operator without mapping behavior")
    public void testIndexFilterWithoutMapping() {
        UnaryOperator<String> operator = new IndexMapperBuilder().build();
        assertNotNull(operator);
        assertNull(operator.apply(DUMMYINDEX));
    }


}
