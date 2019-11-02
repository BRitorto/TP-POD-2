package query5;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query5Collator implements Collator<Map.Entry<String, Double>, List<Map.Entry<String, Double>>>{

    private final int n;

    public Query5Collator(final int n) {
        super();
        this.n = n;
    }

    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Double>> iterable) {
        final List<Map.Entry<String, Double>> sortedEntries = new ArrayList<>();
        iterable.forEach(sortedEntries::add);
        sortedEntries.sort(Comparator
                .comparing(Map.Entry<String, Double>::getValue)
                .thenComparing(Map.Entry::getKey));
        return sortedEntries.subList(0, n);
    }
}
