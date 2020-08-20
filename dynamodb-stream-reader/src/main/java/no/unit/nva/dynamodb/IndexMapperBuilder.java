package no.unit.nva.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

public class IndexMapperBuilder {

    private final Map<String, String> indexMapping = new HashMap<>();
    private boolean passthru;

    public IndexMapperBuilder withIndex(String fromIndex, String toIndex) {
        this.indexMapping.put(fromIndex, toIndex);
        return this;  //By returning the builder each time, we can create a fluent interface.
    }

    public IndexMapperBuilder doPassthru() {
        this.passthru = true;
        return this;
    };


    public UnaryOperator<String> build() {
        if (!passthru) {
            return index -> indexMapping.get(index);
        } else {
            return new UnaryOperator<String>() {
                @Override
                public String apply(String s) {
                    if (indexMapping.containsKey(s)) {
                        return indexMapping.get(s);
                    } else {
                        return s;
                    }
                }
            };
        }
    }
}


