package no.unit.nva.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

public class IndexMapperBuilder {

    private final Map<String, String> indexMapping = new HashMap<>();
    private boolean passthru;

    /**
     * Adds a mapping of an index.
     * @param fromIndex name of index to be renamed
     * @param toIndex new name of index
     * @return Builder to generate the wanted operation
     */
    public IndexMapperBuilder withIndex(String fromIndex, String toIndex) {
        this.indexMapping.put(fromIndex, toIndex);
        return this;
    }

    /**
     * Let this filter alllow all indexes.
     * @return Builder to generate the wanted operation
     */
    public IndexMapperBuilder doPassthru() {
        this.passthru = true;
        return this;
    }

    /**
     * Generate the wanted UnaryOperation.
     * @return UnaryOperation to be used
     */
    public UnaryOperator<String> build() {
        if (!passthru) {
            return index -> indexMapping.get(index);
        } else {
            return index -> indexMapping.getOrDefault(index, index);
        }
    }
}


