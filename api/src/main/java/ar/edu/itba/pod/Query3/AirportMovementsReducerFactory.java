package ar.edu.itba.pod.Query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class AirportMovementsReducerFactory implements ReducerFactory<OaciTuple, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(OaciTuple oaciTuple) {
        return new AirportMovementReducer();
    }

    private class AirportMovementReducer extends Reducer<Integer, Integer> {
        private volatile int sum;

        @Override
        public void beginReduce() {
            sum = 0;
        }

        @Override
        public void reduce(Integer integer) {
            sum += integer;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }
}
