package ar.edu.itba.pod.Query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class InternationalReducerFactory implements ReducerFactory<String, InternationalCountTuple, Integer> {
    @Override
    public Reducer<InternationalCountTuple, Integer> newReducer(String s) {
        return new InternationalReducer();
    }

    private class InternationalReducer extends Reducer<InternationalCountTuple, Integer> {
        private volatile double sum;
        private double total;

        @Override
        public void beginReduce() {
            sum = 0;
            total = 0;
        }

        @Override
        public void reduce(InternationalCountTuple internationalCountTuple) {
            sum += internationalCountTuple.getAccum();
            total += internationalCountTuple.getTotal();
        }

        @Override
        public Integer finalizeReduce() {
            return (int) (sum/total*100);
        }
    }
}

