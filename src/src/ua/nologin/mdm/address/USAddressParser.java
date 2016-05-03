package ua.nologin.mdm.address;/**
 * Created by yaric on 8/18/15.
 */

import com.aliasi.util.Proximity;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Iaroslav Omelianenko
 */
public class USAddressParser {

    public static String[] noise = {"-", ".", "#"};

    private static ConcurrentHashMap<String, USAddress> addrCache = new ConcurrentHashMap<>();

    // 1635 CLIFRON RD. NE, BUILDING A, ATLANTA, GA 30322-0001, US
    public static USAddress parse(String addLine) {
        String[] tokens = addLine.split(",");
        USAddress address = new USAddress();
        address.setStreetWithNumber(tokens[0].trim());
        address.setCity(tokens[tokens.length - 3].trim());
        // find ZIP and state
        parseStateZip(tokens[tokens.length - 2].trim(), address);

        return address;
    }

    public static double compare(USAddress first, USAddress second, Proximity<CharSequence> proximity) {
        // compare street
        double sProx = proximity.proximity(first.getStreetWithNumber(), second.getStreetWithNumber());
        double cProx = proximity.proximity(first.getCity(), second.getCity());
        double stateProx = first.getState().equalsIgnoreCase(second.getState()) ? 1 : 0;
        double zip5Prox = first.getZip5().equalsIgnoreCase(second.getZip5()) ? 1 : 0;

        double res = sProx * .45 + cProx * .25 + stateProx * .25 + zip5Prox * .05;
        return res;
    }

    /**
     * Compares provided address lines and returns probability that it is the same address. This method employs heavy caching
     * of intermediate results.
     *
     * @param firstAddr  the first address line
     * @param secondArrd the second address line
     * @param proximity  the proximity analyzer
     * @return the probability that provided address lines are describe the same postal entity
     */
    public static double compare(String firstAddr, String secondArrd, Proximity<CharSequence> proximity) {
        // try to get parsed addresses from cache first
        USAddress first = addrCache.get(firstAddr);
        if (first == null) {
            first = parse(firstAddr);
            addrCache.put(firstAddr, first);
        }
        USAddress second = addrCache.get(secondArrd);
        if (second == null) {
            second = parse(secondArrd);
            addrCache.put(secondArrd, second);
        }
        return compare(first, second, proximity);
    }

    public static String filterNoise(String addrLine) {
        for (String s : noise) {
            if (addrLine.startsWith(s)) {
                addrLine = addrLine.replace(s, "");
                return addrLine.trim();
            }
        }
        return addrLine.trim();
    }

    // GA 30322-0001
    private static void parseStateZip(String line, USAddress address) {
        String[] tokens = line.split(" ");
        address.setState(tokens[0].trim());
        int hyphenIndex = tokens[1].indexOf('-');
        if (hyphenIndex != -1) {
            address.setZip5(tokens[1].substring(0, hyphenIndex));
            address.setZip4(tokens[1].substring(hyphenIndex + 1, tokens[1].length()));
        } else {
            address.setZip5(tokens[1].trim());
        }
    }
}
