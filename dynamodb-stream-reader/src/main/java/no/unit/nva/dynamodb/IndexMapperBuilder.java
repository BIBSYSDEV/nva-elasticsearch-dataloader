package no.unit.nva.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

public class IndexMapperBuilder {

    private Map<String, String> indexMapping;

    public IndexMapperBuilder withIndex(String fromIndex, String toIndex) {
        if (this.indexMapping == null) {
            this.indexMapping = new HashMap<>();
        }
        this.indexMapping.put(fromIndex, toIndex);
        return this;  //By returning the builder each time, we can create a fluent interface.
    }


    public UnaryOperator<String> build() {
            return index -> indexMapping.get(index);
    }
}


