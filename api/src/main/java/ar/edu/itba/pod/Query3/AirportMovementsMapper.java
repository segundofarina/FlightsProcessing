package ar.edu.itba.pod.Query3;

import ar.edu.itba.pod.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class AirportMovementsMapper implements Mapper<String, Movement, OaciTuple, Integer> {

    @Override
    public void map(String s, Movement movement, Context<OaciTuple, Integer> context) {
        String sourceOasi = movement.getSourceOASI();
        String destinationOasi = movement.getDestinationOASI();

        if(!sourceOasi.equalsIgnoreCase("N/A") && !destinationOasi.equalsIgnoreCase("N/A") && !sourceOasi.equalsIgnoreCase(destinationOasi)) {
            context.emit(new OaciTuple(sourceOasi, destinationOasi), 1);
        }
    }
}
