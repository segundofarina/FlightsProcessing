package ar.edu.itba.pod.Query5;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class InternationalCombinerFactory implements CombinerFactory<String, Integer, InternationalCountTuple> {

    @Override
    public Combiner<Integer, InternationalCountTuple> newCombiner(String s) {
        return new InternationalCombiner();
    }

    private class InternationalCombiner extends Combiner<Integer, InternationalCountTuple> {
        private int accum = 0;
        private int total = 0;

        @Override
        public void combine(Integer integer) {
            accum += integer;
            total++;
        }

        @Override
        public InternationalCountTuple finalizeChunk() {
            return new InternationalCountTuple(accum, total);
        }

        @Override
        public void reset() {
            accum = 0;
            total = 0;
        }
    }
}
