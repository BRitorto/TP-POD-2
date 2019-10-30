package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Optional;

public class Movement implements DataSerializable {

    Optional<FlightEnum> flightType;
    MovementEnum movementType;
    String startOACI;
    String endOACI;
    String airlineName;

    public Movement(){ }

    public Movement(Optional<FlightEnum> flightType, MovementEnum movementType, String startOACI, String endOACI, String airlineName) {
        this.flightType = flightType;
        this.movementType = movementType;
        this.startOACI = startOACI;
        this.endOACI = endOACI;
        this.airlineName = airlineName;
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

    public String getAirlineName() {
        return airlineName;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(flightType.map(FlightEnum::name).orElse("NULL"));
        objectDataOutput.writeUTF(movementType.name());
        objectDataOutput.writeUTF(startOACI);
        objectDataOutput.writeUTF(endOACI);
        objectDataOutput.writeUTF(airlineName);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        flightType = Optional.of(objectDataInput.readUTF()).map(s ->  s.equals("NULL") ? null : FlightEnum.valueOf(s));
        movementType = MovementEnum.valueOf(objectDataInput.readUTF());
        startOACI = objectDataInput.readUTF();
        endOACI = objectDataInput.readUTF();
        airlineName = objectDataInput.readUTF();
    }
}
