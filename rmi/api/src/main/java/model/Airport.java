package model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class Airport implements DataSerializable {

    private Optional<String> oaci;
    String name;
    String province;

    public Airport(Optional<String> oaci, String name, String province){
        this.oaci = oaci;
        this.name = name;
        this.province = province;
    }

    public Optional<String> getOaci() {
        return oaci;
    }

    public String getName() {
        return name;
    }


    public String getProvince() {
        return province;
    }

    @Override
    public String toString() {
        return "Airport{" +
                "oaci='" + oaci + '\'' +
                ", name='" + name + '\'' +
                ", province='" + province + '\'' +
                '}';
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(oaci.orElse("NULL"));
        objectDataOutput.writeUTF(name);
        objectDataOutput.writeUTF(province);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        oaci = Optional.of(objectDataInput.readUTF()).filter(s -> !"NULL".equals(s));
        name = objectDataInput.readUTF();
        province = objectDataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Airport)) return false;
        Airport airport = (Airport) o;
        return Objects.equals(getOaci(), airport.getOaci()) &&
                Objects.equals(getName(), airport.getName()) &&
                Objects.equals(getProvince(), airport.getProvince());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getOaci(), getName(), getProvince());
    }
}
