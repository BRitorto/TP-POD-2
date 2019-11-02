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
    FlightClassEnum flightClass;

    public Movement(){ }

    public Movement(Optional<FlightEnum> flightType, MovementEnum movementType, String startOACI, String endOACI, String airlineName,
                    FlightClassEnum flightClass) {
        this.flightType = flightType;
        this.movementType = movementType;
        this.startOACI = startOACI;
        this.endOACI = endOACI;
        this.airlineName = airlineName;
        this.flightClass = flightClass;
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

    public FlightClassEnum getFlightClass() {
        return flightClass;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(flightType.map(FlightEnum::name).orElse("NULL"));
        objectDataOutput.writeUTF(movementType.name());
        objectDataOutput.writeUTF(startOACI);
        objectDataOutput.writeUTF(endOACI);
        objectDataOutput.writeUTF(airlineName);
        objectDataOutput.writeUTF(flightClass.name());
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        flightType = Optional.of(objectDataInput.readUTF()).map(s ->  s.equals("NULL") ? null : FlightEnum.valueOf(s));
        movementType = MovementEnum.valueOf(objectDataInput.readUTF());
        startOACI = objectDataInput.readUTF();
        endOACI = objectDataInput.readUTF();
        airlineName = objectDataInput.readUTF();
        flightClass = FlightClassEnum.valueOf(objectDataInput.readUTF());
    }
}
