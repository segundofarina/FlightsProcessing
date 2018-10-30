package ar.edu.itba.pod;

import java.io.Serializable;
import java.util.Optional;

public class Movement implements Serializable {
    //private final Optional<FlightType> flightType;
    private final FlightType flightType;
    private final MovementType movementType;
    private final String sourceOASI;
    private final String destinationOASI;

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
}
