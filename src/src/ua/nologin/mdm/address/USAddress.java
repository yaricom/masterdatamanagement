package ua.nologin.mdm.address;/**
 * Created by yaric on 8/18/15.
 */

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * The simplified representation of US postal address.
 * @author Iaroslav Omelianenko
 */
public class USAddress {
    private String streetWithNumber;
    private String city;
    private String state;
    private String zip5;
    private String zip4;

    public USAddress(){}

    public USAddress(String streetWithNumber, String city, String state, String zip5, String zip4) {
        this.streetWithNumber = streetWithNumber;
        this.city = city;
        this.state = state;
        this.zip5 = zip5;
        this.zip4 = zip4;
    }

    public void setStreetWithNumber(String streetWithNumber) {
        this.streetWithNumber = streetWithNumber;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setZip5(String zip5) {
        if (zip5.length() < 5) {
            // insert 0 at the beginning
            StringBuilder buf = new StringBuilder(zip5);
            while (buf.length() < 5) {
                buf.insert(0, '0');
            }
            this.zip5 = buf.toString();
        } else {
            this.zip5 = zip5;
        }
    }

    public void setZip4(String zip4) {
        this.zip4 = zip4;
    }

    public String getStreetWithNumber() {
        return streetWithNumber;
    }

    public String getCity() {
        return city;
    }

    public String getState() {
        return state;
    }

    public String getZip5() {
        return zip5;
    }

    public String getZip4() {
        return zip4;
    }

    public boolean equals(Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
            return false;
        }
        USAddress rhs = (USAddress) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(streetWithNumber, rhs.streetWithNumber)
                .append(city, rhs.city)
                .append(state, rhs.state)
                .append(zip5, rhs.zip5)
                .isEquals();
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 37).
                append(streetWithNumber).
                append(city).
                append(state).
                append(zip5).
                toHashCode();
    }
}
