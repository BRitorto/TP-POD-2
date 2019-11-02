package query4;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query4Collator implements Collator<Map.Entry<String,Integer>, List<Map.Entry<String,Integer>>> {
    private final int limit;

    public Query4Collator(final int limit) {
        this.limit = limit;
    }

    @Override
    public List<Map.Entry<String, Integer>> collate(Iterable<Map.Entry<String, Integer>> values) {
        List<Map.Entry<String,Integer>> aux = new ArrayList<>();

        for(Map.Entry<String,Integer> value: values){
            aux.add(value);
        }
        aux.sort(new customComparator());
        return aux.subList(0, Math.min(aux.size(), limit));
    }

    private static class customComparator implements Comparator<Map.Entry<String,Integer>> {
        @Override
        public int compare(Map.Entry<String, Integer> object1, Map.Entry<String, Integer> object2) {
            return Integer.compare(object2.getValue(), object1.getValue());
        }
    }
}
