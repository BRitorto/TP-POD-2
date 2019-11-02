package query6;

import java.io.Serializable;

public class ProvinceTuple implements Serializable {

    private String province1;
    private String province2;

    public ProvinceTuple(String province1, String province2) {
        this.province1 = province1;
        this.province2 = province2;
    }

    public void setProvince1(String province1) {
        this.province1 = province1;
    }

    public void setProvince2(String province2) {
        this.province2 = province2;
    }

    public String getProvince1() {
        return province1;
    }

    public String getProvince2() {
        return province2;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj){
            return true;
        }
        if(obj == null || !(obj instanceof ProvinceTuple)){
            return false;
        }
        ProvinceTuple o = (ProvinceTuple) obj;
        return (this.province1.equals(o.province1) && this.province2.equals(o.province2)) ||
                (this.province1.equals(o.province2) && this.province2.equals(o.province1));
    }

    @Override
    public int hashCode() {
        int res = 7;
        boolean cmp = province1.compareTo(province2) >= 0;
        res = 31 * res + (cmp ? province1.hashCode() : province2.hashCode());
        res = 31 * res + (cmp ? province2.hashCode() : province1.hashCode());
        return res;
    }
}
