package query2;

import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query2Collator implements Collator<Map.Entry<String, Integer>, List<Map.Entry<String, Double>>>{

    private Integer n;

    public Query2Collator(Integer n) {
        this.n = n;
    }

    private Long computeTotal(Iterable<Map.Entry<String, Integer>> iterable){
        Long total = 0L;
        for(Map.Entry<String, Integer> airline: iterable){
            total += airline.getValue();
        }
        return total;
    }

    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        Map<String, Double> percentages = new HashMap<>();
        Long total = computeTotal(iterable);

        for(Map.Entry<String, Integer> airline: iterable){
            percentages.put(airline.getKey(),(100.00 * airline.getValue()) / (double)total);
        }

        percentages.remove("N/A");

        final List<Map.Entry<String, Double>> resultAux = new ArrayList<>(percentages.entrySet());
        resultAux.sort(Comparator.comparing(Map.Entry<String, Double>::getValue).reversed());
        final List<Map.Entry<String, Double>> result = new ArrayList<>(resultAux.subList(0,n));
        resultAux.clear();

        double otherPercentage = 0.0;
        otherPercentage += result.stream().mapToDouble(Map.Entry::getValue).sum();
        otherPercentage = 100.00 - otherPercentage;
        Map.Entry<String, Double> othersEntry = new MapEntrySimple<>("Otros",otherPercentage);
        result.add(othersEntry);

        return result;
    }
}
