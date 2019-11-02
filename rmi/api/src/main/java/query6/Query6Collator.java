package query6;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query6Collator implements Collator<Map.Entry<ProvinceTuple, Integer>, Set<Map.Entry<ProvinceTuple, Integer>>> {

    private Integer min;

    public Query6Collator(Integer min) {
        this.min = min;
    }

    @Override
    public Set<Map.Entry<ProvinceTuple, Integer>> collate(Iterable<Map.Entry<ProvinceTuple, Integer>> iterable) {
        SortedSet<Map.Entry<ProvinceTuple, Integer>> result = new TreeSet<>(Comparator.comparing(Map.Entry::getValue));
        for(Map.Entry<ProvinceTuple, Integer> provinceTuple: iterable){
            Integer movements = provinceTuple.getValue();
            if(min <= movements){
                String province1 = provinceTuple.getKey().getProvince1();
                String province2 = provinceTuple.getKey().getProvince2();
                if(province2.compareTo(province1) < 0){
                    String aux = province1;
                    provinceTuple.getKey().setProvince1(province2);
                    provinceTuple.getKey().setProvince2(aux);
                }
                result.add(provinceTuple);
            }
        }
        return result;
    }
}
