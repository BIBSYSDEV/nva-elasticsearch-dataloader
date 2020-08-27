package no.unit.nva.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

public class IndexMapperBuilder {

    private final Map<String, String> indexMapping = new HashMap<>();
    private boolean passThrough;

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
    public IndexMapperBuilder doPassThrough() {
        this.passThrough = true;
        return this;
    }

    /**
     * Generate the wanted UnaryOperation.
     * @return UnaryOperation to be used
     */
    public UnaryOperator<String> build() {
        if (!passThrough) {
            return index -> indexMapping.get(index);
        } else {
            return index -> indexMapping.getOrDefault(index, index);
        }
    }
}


