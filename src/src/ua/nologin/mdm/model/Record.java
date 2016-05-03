package ua.nologin.mdm.model;/**
 * Created by yaric on 8/13/15.
 */

/**
 * @author Iaroslav Omelianenko
 */
public class Record {
    private int id;
    private String name;
    private String address;
    private String taxonomies;

    public Record(int id, String name, String address, String taxonomies) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.taxonomies = taxonomies;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append("|").append(name).append("|").append(address).append("|").append(taxonomies);
        return sb.toString();
    }
}
