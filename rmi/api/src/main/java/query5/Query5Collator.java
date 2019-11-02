package query5;

import com.hazelcast.core.IList;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.mapreduce.Collator;
import model.Airport;

import java.util.*;

public class Query5Collator implements Collator<Map.Entry<String, Integer>, List<Map.Entry<String, Double>>>{

    private Integer n;
    private Integer total;

    public Query5Collator(Integer n, Integer total) {
        this.n = n;
        this.total = total;
    }

    private Long computeTotal(Iterable<Map.Entry<String, Integer>> iterable){
        Long total = 0L;
        for(Map.Entry<String, Integer> oaci: iterable){
            total += oaci.getValue();
        }
        return total;
    }

    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        Map<String, Double> percentages = new HashMap<>();
//        Long total = computeTotal(iterable);

        for(Map.Entry<String, Integer> oaci: iterable){
            percentages.put(oaci.getKey(),(100.00 * oaci.getValue()) / (double)total);
        }

        percentages.remove("N/A");

//        final List<Map.Entry<String, Double>> result = new ArrayList<>(percentages.entrySet());
        final List<Map.Entry<String, Double>> resultAux = new ArrayList<>(percentages.entrySet());
        resultAux.sort(Comparator.comparing(Map.Entry<String, Double>::getValue));
//        final List<Map.Entry<String, Double>> result = new ArrayList<>(resultAux.subList(0,n));
//        resultAux.clear();

//        double otherPercentage = 0.0;
//        otherPercentage += result.stream().mapToDouble(Map.Entry::getValue).sum();
//        otherPercentage = 100.00 - otherPercentage;
//        Map.Entry<String, Double> othersEntry = new MapEntrySimple<>("Otros",otherPercentage);
//        result.add(othersEntry);

        return resultAux;
    }
}
