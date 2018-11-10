package ar.edu.itba.pod;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Optional;

public class Movement implements DataSerializable {
    private  Optional<FlightType> flightType;
    private MovementType movementType;
    private String sourceOASI;
    private String destinationOASI;

    public Movement(){

    }
    public Movement(Optional<FlightType> flightType, MovementType movementType, String sourceOASI, String destinationOASI) {
        this.flightType = flightType;
        this.movementType = movementType;
        this.sourceOASI = sourceOASI;
        this.destinationOASI = destinationOASI;
    }

    public Optional<FlightType> getFlightType() {
        return flightType;
    }

    public MovementType getMovementType() {
        return movementType;
    }

    public String getSourceOASI() {
        return sourceOASI;
    }

    public String getDestinationOASI() {
        return destinationOASI;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(flightType.map(FlightType::name).orElse("NULL"));
        out.writeUTF(movementType.name());
        out.writeUTF(sourceOASI);
        out.writeUTF(destinationOASI);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        flightType = Optional.of(in.readUTF()).map(s ->  s.equals("NULL") ? null : FlightType.valueOf(s));
        movementType = MovementType.valueOf(in.readUTF());
        sourceOASI = in.readUTF();
        destinationOASI = in.readUTF();
    }
}
