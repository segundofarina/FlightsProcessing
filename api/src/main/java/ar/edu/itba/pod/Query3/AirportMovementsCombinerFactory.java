package ar.edu.itba.pod.Query3;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class AirportMovementsCombinerFactory implements CombinerFactory<OaciTuple, Integer, Integer> {
    @Override
    public Combiner<Integer, Integer> newCombiner(OaciTuple oaciTuple) {
        return new AirportMovementsCombiner();
    }

    private class AirportMovementsCombiner extends Combiner<Integer, Integer> {
        private int sum = 0;

        @Override
        public void combine(Integer integer) {
            sum += integer;
        }

        @Override
        public Integer finalizeChunk() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }
}
