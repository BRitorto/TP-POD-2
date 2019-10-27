import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Optional;

public class Flight implements DataSerializable{
    private Optional<Destination> destination;
    private FlightPhase flightPhase;
    private String departureOASI;
    private String arrivalOASI;

    public Flight(){

    }
    public Flight(Optional<Destination> destination, FlightPhase flightPhase, String departureOASI, String arrivalOASI) {
        this.destination = destination;
        this.flightPhase = flightPhase;
        this.departureOASI = departureOASI;
        this.arrivalOASI = arrivalOASI;
    }

    public Optional<Destination> getDestinationType() {
        return destination;
    }

    public void setDestinationType(Optional<Destination> destination) {
        this.destination = destination;
    }

    public FlightPhase getFlightPhase() {
        return flightPhase;
    }

    public void setFlightPhase(FlightPhase flightPhase) {
        this.flightPhase = flightPhase;
    }

    public String getDepartureOASI() {
        return departureOASI;
    }

    public void setDepartureOASI(String departureOASI) {
        this.departureOASI = departureOASI;
    }

    public String getArrivalOASI() {
        return arrivalOASI;
    }

    public void setArrivalOASI(String arrivalOASI) {
        this.arrivalOASI = arrivalOASI;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(destination.map(FlightPhase::name).orElse("NULL"));
        out.writeUTF(flightPhase.name());
        out.writeUTF(departureOASI);
        out.writeUTF(arrivalOASI);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {

    }
}
