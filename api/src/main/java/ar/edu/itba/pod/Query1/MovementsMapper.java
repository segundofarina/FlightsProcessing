package ar.edu.itba.pod.Query1;

import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.MovementType;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class MovementsMapper implements Mapper<String, Movement, String, Integer> {
    @Override
    public void map(String s, Movement movement, Context<String, Integer> context) {
        if(movement.getMovementType() == MovementType.TAKE_OF) {
            context.emit(movement.getSourceOASI(), 1);
        }
        if(movement.getMovementType() == MovementType.LANDING) {
            context.emit(movement.getDestinationOASI(), 1);
        }
    }
}
