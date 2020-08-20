package no.unit.nva.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

public class IndexMapperBuilder {

    private final Map<String, String> indexMapping = new HashMap<>();
    private boolean passthru;

    public IndexMapperBuilder withIndex(String fromIndex, String toIndex) {
        this.indexMapping.put(fromIndex, toIndex);
        return this;
    }

    public IndexMapperBuilder doPassthru() {
        this.passthru = true;
        return this;
    };


    public UnaryOperator<String> build() {
        if (!passthru) {
            return index -> indexMapping.get(index);
        } else {
            return index -> indexMapping.getOrDefault(index, index);
        }
    }
}


