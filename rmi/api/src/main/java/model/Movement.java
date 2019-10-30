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

    public Movement(){ }

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

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(flightType.map(FlightEnum::name).orElse("NULL"));
        objectDataOutput.writeUTF(movementType.name());
        objectDataOutput.writeUTF(startOACI);
        objectDataOutput.writeUTF(endOACI);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        flightType = Optional.of(objectDataInput.readUTF()).map(s ->  s.equals("NULL") ? null : FlightEnum.valueOf(s));
        movementType = MovementEnum.valueOf(objectDataInput.readUTF());
        startOACI = objectDataInput.readUTF();
        endOACI = objectDataInput.readUTF();
    }
}
