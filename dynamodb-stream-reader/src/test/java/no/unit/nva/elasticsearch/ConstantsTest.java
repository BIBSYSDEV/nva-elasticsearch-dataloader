package no.unit.nva.elasticsearch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConstantsTest {

    @Test
    public void createInstanceOfConstansToGetCodeCoverage() {
        Constants constants = new Constants();
        assertNotNull(constants);
    }

}
