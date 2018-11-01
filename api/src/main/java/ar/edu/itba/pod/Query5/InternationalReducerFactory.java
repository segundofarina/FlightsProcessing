package ar.edu.itba.pod.Query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class InternationalReducerFactory implements ReducerFactory<String, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new InternationalReducer();
    }

    private class InternationalReducer extends Reducer<Integer, Integer> {
        private volatile double sum;
        private double total;

        @Override
        public void beginReduce() {
            sum = 0;
            total =0;
        }

        @Override
        public void reduce(Integer integer) {
            sum += integer;
            total++;
        }

        @Override
        public Integer finalizeReduce() {
            return (int)(sum/total*100);
        }
    }
}

