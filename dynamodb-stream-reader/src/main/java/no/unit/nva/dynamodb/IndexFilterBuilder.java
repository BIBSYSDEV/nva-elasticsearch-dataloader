package no.unit.nva.dynamodb;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

public class IndexFilterBuilder {

    private boolean allowAll;
    private Set<String> wantedIndexes;

    public IndexFilterBuilder withIndex(String index) {
        if (this.wantedIndexes == null) {
            this.wantedIndexes = new HashSet<>();
        }
        this.wantedIndexes.add(index);
        return this;  //By returning the builder each time, we can create a fluent interface.
    }

    public IndexFilterBuilder doAllowAll() {
        this.allowAll = true;
        return this;
    }

    public Predicate<String> build() {
        if (allowAll) {
            return new Predicate<String>() {
                @Override
                public boolean test(String t) {
                    return true;
                }
            };
        } else {
            return new Predicate<String>() {
                @Override
                public boolean test(String index) {
                    return wantedIndexes.contains(index);
                }
            };
        }
    }
}


