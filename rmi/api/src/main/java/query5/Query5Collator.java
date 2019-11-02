package query5;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query5Collator implements Collator<Map.Entry<String, Double>, List<Map.Entry<String, Double>>>{

    private final int n;

    public Query5Collator(final int n) {
        this.n = n;
    }

    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Double>> iterable) {
        final List<Map.Entry<String, Double>> percentages = new ArrayList<>();
        iterable.forEach(percentages::add);
        percentages.sort(Comparator.comparing(Map.Entry<String, Double>::getValue)
                .thenComparing(Map.Entry::getKey));

        percentages.remove("N/A");

        final List<Map.Entry<String, Double>> result = new ArrayList<>(percentages.subList(0,n));
        percentages.clear();

        return result;
    }
}
