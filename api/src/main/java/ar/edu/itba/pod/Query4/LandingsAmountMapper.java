package ar.edu.itba.pod.Query4;

import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.MovementType;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class LandingsAmountMapper implements Mapper<String, Movement, String, Integer> {
    private final String destinationOaci;

    public LandingsAmountMapper(String destinationOasi) {
        this.destinationOaci = destinationOasi;
    }

    @Override
    public void map(String s, Movement movement, Context<String, Integer> context) {
        if(movement.getMovementType() == MovementType.LANDING && movement.getDestinationOASI().equalsIgnoreCase(destinationOaci)) {
            context.emit(movement.getSourceOASI(), 1);
        }
    }
}
