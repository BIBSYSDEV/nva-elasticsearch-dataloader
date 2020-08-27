package no.unit.nva.dynamodb;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

public class IndexFilterBuilder {

    private boolean allowAll;
    private final Set<String> wantedIndexes = new HashSet<>();

    /**
     * Adds index to set of wanted indexes.
     * @param index to be added
     * @return builder to use
     */
    public IndexFilterBuilder withIndex(String index) {
        wantedIndexes.add(index);
        return this;  //By returning the builder each time, we can create a fluent interface.
    }

    /**
     * Make this filer allow all index names.
     * @return builder to make filter with
     */
    public IndexFilterBuilder doAllowAll() {
        this.allowAll = true;
        return this;
    }

    /**
     * Build the filer.
     * @return predicate to be used
     */
    public Predicate<String> build() {
        if (allowAll) {
            return t -> true;
        } else {
            return index -> wantedIndexes.contains(index);
        }
    }
}


