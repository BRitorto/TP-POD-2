package model;

import java.util.Optional;

public class Movement {

    Optional<FlightEnum> flightType;
    MovementEnum movementType;
    String startOACI;
    String endOACI;

    public Movement(){

    }

    public Movement(Optional<FlightEnum> flightType, MovementEnum movementType, String startOACI, String endOACI) {
        this.flightType = flightType;
        this.movementType = movementType;
        this.startOACI = startOACI;
        this.endOACI = endOACI;
    }

    public Optional<FlightEnum> getFlightType() {
        return flightType;
    }

    public MovementEnum getMovementType() {
        return movementType;
    }

    public String getStartOACI() {
        return startOACI;
    }

    public String getEndOACI() {
        return endOACI;
    }
}
