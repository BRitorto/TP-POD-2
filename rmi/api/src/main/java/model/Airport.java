package model;

public class Airport {

    String oaci;
    String name;
    String province;

    public Airport(String oaci, String name, String province){
        this.oaci = oaci;
        this.name = name;
        this.province = province;
    }

    public String getOaci() {
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
}
