package ar.edu.itba.pod;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

public class Movement implements DataSerializable {
    //private final Optional<FlightType> flightType;
    private FlightType flightType;
    private MovementType movementType;
    private String sourceOASI;
    private String destinationOASI;

    public Movement(){

    }
    //public Movement(Optional<FlightType> flightType, MovementType movementType, String sourceOASI, String destinationOASI) {
    public Movement(FlightType flightType, MovementType movementType, String sourceOASI, String destinationOASI) {
        this.flightType = flightType;
        this.movementType = movementType;
        this.sourceOASI = sourceOASI;
        this.destinationOASI = destinationOASI;
    }

    //public Optional<FlightType> getFlightType() {
    public FlightType getFlightType() {
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
        out.writeObject(flightType);
        out.writeObject(movementType);
        out.writeUTF(sourceOASI);
        out.writeUTF(destinationOASI);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        flightType = in.readObject();
        movementType = in.readObject();
        sourceOASI = in.readUTF();
        destinationOASI = in.readUTF();
    }
}
