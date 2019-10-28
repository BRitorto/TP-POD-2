package model;

public class Movement {

    FlightEnum flightType;
    MovementEnum movementType;
    String startOACI;
    String endOACI;

    public Movement(){

    }

    public Movement(FlightEnum flightType, MovementEnum movementType, String startOACI, String endOACI) {
        this.flightType = flightType;
        this.movementType = movementType;
        this.startOACI = startOACI;
        this.endOACI = endOACI;
    }

    public FlightEnum getFlightType() {
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
