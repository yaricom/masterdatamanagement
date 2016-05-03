package ua.nologin.mdm;/**
 * Created by yaric on 8/19/15.
 */

import com.aliasi.spell.JaroWinklerDistance;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import ua.nologin.mdm.address.USAddressParser;
import ua.nologin.mdm.converters.CSVSaver;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static ua.nologin.mdm.Defines.*;

/**
 * The full data set fields comparator in order to extract similar records along with proximity
 *
 * @author Iaroslav Omelianenko
 */
public class FullFieldsComparator {

    private final double fullThreshold;
    private final double addrThreshold;

    private Logger logger = Logger.getLogger("ua.nologin.mdm");

    // The string proximity calculator
    static JaroWinklerDistance distance = JaroWinklerDistance.JARO_DISTANCE;

    public FullFieldsComparator(double fullThreshold, double addrThreshold) {
        this.fullThreshold = fullThreshold;
        this.addrThreshold = addrThreshold;

        FileHandler fh = null;
        try {
            fh = new FileHandler("comparator.txt");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to initialize logger");
        }
        // Send logger output to our FileHandler.
        logger.addHandler(fh);
        // Request that every detail gets logged.
        logger.setLevel(Level.ALL);
    }

    public void compareRecords(File input, File namesMatrix, File addrMatrix, File results) throws IOException, ClassNotFoundException {
        List<List<Object>> dataSet = UtilsIO.loadCSVAsDataSet(input);
        Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByNames = UtilsIO.loadMatrix(namesMatrix);
        Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByAddr = UtilsIO.loadMatrix(addrMatrix);

        List<List<Object>> records = this.compareRecords(dataSet, nerMatrixByNames, nerMatrixByAddr);

        CSVSaver.FieldType[] types = {CSVSaver.FieldType.INT, CSVSaver.FieldType.INT, CSVSaver.FieldType.DOUBLE};
        UtilsIO.saveCSVDataSet(records, types, results);
    }

    public void compareRecords(File input, File namesMatrix, File results) throws IOException, ClassNotFoundException {
        List<List<Object>> dataSet = UtilsIO.loadCSVAsDataSet(input);
        Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByNames = UtilsIO.loadMatrix(namesMatrix);

        List<List<Object>> records = this.compareRecords(dataSet, nerMatrixByNames);

        CSVSaver.FieldType[] types = {CSVSaver.FieldType.INT, CSVSaver.FieldType.INT, CSVSaver.FieldType.DOUBLE};
        UtilsIO.saveCSVDataSet(records, types, results);
    }

    public List<List<Object>> compareRecords(List<List<Object>> dataSet, Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByNames) {
        // convert data set into map keyed by record ID
        TreeMap<Integer, List<Object>> dataMap = new TreeMap<>();
        for (List<Object> row : dataSet) {
            Integer id = ((Double) row.get(ID_INDEX)).intValue();
            dataMap.put(id, row);
        }

        System.out.println("Starting full fields compare +++++++++++++");
        // do processing by NAME
        int splitFactor = 20;
        // get NER keys sorted
        Set<ImmutablePair<Integer, Integer>> keySet = nerMatrixByNames.keySet();
        ArrayList<ImmutablePair<Integer, Integer>> nerKeys = new ArrayList<>(keySet);
        nerKeys.sort(new NerKeysComparator());
        keySet = null;

        ImmutablePair<Integer, Integer> lastKey = nerKeys.get(nerKeys.size() - 1);
        System.out.printf("Last NER pair: %d : %d\n", lastKey.left, lastKey.right);

        int size = nerMatrixByNames.size();
        FullRecordsCompareByNameTask nameTask = new FullRecordsCompareByNameTask(dataMap, nerMatrixByNames, nerKeys,
                0, size, size / splitFactor);
        ForkJoinPool pool = new ForkJoinPool();
        TreeMap<ImmutablePair<Integer, Integer>, Double> resultByName = pool.invoke(nameTask);


        // filter results by removing excesses
        ArrayList<List<Object>>resultSet = this.filteredRecords(resultByName, dataMap);

        logger.info(String.format("Found %d duplicate records in total\n", resultSet.size()));

        // sort
//        Collections.sort(resultSet, new RecordsResultComparator());

        return resultSet;
    }

    private ArrayList<List<Object>> filteredRecords(TreeMap<ImmutablePair<Integer, Integer>, Double> resultsMap,
                                                    TreeMap<Integer, List<Object>> dataMap) {
        ArrayList<List<Object>>resultSet = new ArrayList<>(resultsMap.size());
        ImmutablePair<Integer, Integer> prevKey = resultsMap.navigableKeySet().first();
        ArrayList<ImmutablePair<Integer, Integer>>keys = new ArrayList<>();
        int count = 0;
        for (ImmutablePair<Integer, Integer> key : resultsMap.navigableKeySet()) {
            if (Objects.equals(prevKey.left, key.left)) {
                count++;
            } else {
                if (count <= 3) {
                    this.addAll(keys, resultSet, resultsMap, dataMap);
                }
                keys = new ArrayList<>();
                count = 0;
                prevKey = key;
            }
            // store key
            keys.add(key);

        }
        return resultSet;
    }

    private void addAll(ArrayList<ImmutablePair<Integer, Integer>>keys, ArrayList<List<Object>>resultSet,
                        TreeMap<ImmutablePair<Integer, Integer>, Double> resultsMap,
                        TreeMap<Integer, List<Object>> dataMap) {
        List<Object> firstRecord, secRecord;
        for (ImmutablePair<Integer, Integer> key : keys) {
            resultSet.add(Arrays.asList(Double.valueOf(key.left), Double.valueOf(key.right), resultsMap.get(key)));
            firstRecord = dataMap.get(key.left);
            secRecord = dataMap.get(key.right);
            logger.info(String.format("%s | %s | %s | %s \n%s | %s | %s | %s\n++++++++++++++++++++++++++++++\n",
                    firstRecord.get(ID_INDEX), firstRecord.get(NAME_INDEX), firstRecord.get(ADDR_INDEX), firstRecord.get(TAXONOM_INDEX),
                    secRecord.get(ID_INDEX), secRecord.get(NAME_INDEX), secRecord.get(ADDR_INDEX), secRecord.get(TAXONOM_INDEX)));
        }
    }

    public List<List<Object>> compareRecords(List<List<Object>> dataSet, Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByNames,
                                             Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByAddr) {
        // convert data set into map keyed by record ID
        TreeMap<Integer, List<Object>> dataMap = new TreeMap<>();
        for (List<Object> row : dataSet) {
            Integer id = ((Double) row.get(ID_INDEX)).intValue();
            dataMap.put(id, row);
        }

        System.out.println("Starting full fields compare +++++++++++++");
        // do processing by NAME
        int splitFactor = 20;
        // get NER keys sorted
        Set<ImmutablePair<Integer, Integer>> keySet = nerMatrixByNames.keySet();
        ArrayList<ImmutablePair<Integer, Integer>> nerKeys = new ArrayList<>(keySet);
        nerKeys.sort(new NerKeysComparator());
        keySet = null;

        ImmutablePair<Integer, Integer> lastKey = nerKeys.get(nerKeys.size() - 1);
        System.out.printf("Last NER pair: %d : %d\n", lastKey.left, lastKey.right);

        int size = nerMatrixByNames.size();
        FullRecordsCompareByNameTask nameTask = new FullRecordsCompareByNameTask(dataMap, nerMatrixByNames, nerKeys,
                0, size, size / splitFactor);
        ForkJoinPool pool = new ForkJoinPool();
        TreeMap<ImmutablePair<Integer, Integer>, Double> resultByName = pool.invoke(nameTask);

        System.out.printf("Found %d duplicate records by NAME\n", resultByName.size());

        // release resources
        nerMatrixByNames = null;
        nerKeys = null;

        // do processing by ADDRESS
        keySet = nerMatrixByAddr.keySet();
        nerKeys = new ArrayList<>(keySet);
        nerKeys.sort(new NerKeysComparator());
        keySet = null;

        size = nerMatrixByAddr.size();
        FullRecordsCompareByAddressTask addrTask = new FullRecordsCompareByAddressTask(dataMap, nerMatrixByAddr, nerKeys, 0,
                size, size / splitFactor);
        TreeMap<ImmutablePair<Integer, Integer>, Double> resultByAddr = pool.invoke(addrTask);

        System.out.printf("Found %d duplicate records by ADDRESS\n", resultByAddr.size());

        // release resources
        nerMatrixByAddr = null;
        nerKeys = null;

        // merge collected
        for (ImmutablePair<Integer, Integer> key : resultByAddr.navigableKeySet()) {
            if (!resultByName.containsKey(key)) {
                // add object
                resultByName.put(key, resultByAddr.get(key));
            }
        }
        resultByAddr = null;

        ArrayList<List<Object>>resultSet = this.filteredRecords(resultByName, dataMap);


        System.out.printf("Found %d duplicate records in total\n", resultSet.size());

        // sort
//        Collections.sort(resultSet, new RecordsResultComparator());

        return resultSet;
    }

    // Task to recursively compare records by name fields using Fork-Join thread
    class FullRecordsCompareByNameTask extends RecursiveTask<TreeMap<ImmutablePair<Integer, Integer>, Double>> {
        // The RAW records
        private final TreeMap<Integer, List<Object>> dataMap;
        // The sorted NER keys
        private final ArrayList<ImmutablePair<Integer, Integer>> sortedNamesNerKeys;
        // The NER results matrix
        private final Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByNames;

        // The start index (inclusive)
        private final int start;
        // The length of data train to process
        private final int lenght;
        // The minimal size of one chunk
        private final int chunkSize;


        public FullRecordsCompareByNameTask(TreeMap<Integer, List<Object>> dataMap, Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByNames,
                                            ArrayList<ImmutablePair<Integer, Integer>> sortedNamesNerKeys, int start, int lenght, int chunkSize) {
            this.dataMap = dataMap;
            this.nerMatrixByNames = nerMatrixByNames;
            this.sortedNamesNerKeys = sortedNamesNerKeys;
            this.start = start;
            this.lenght = lenght;
            this.chunkSize = chunkSize;
        }


        @Override
        protected TreeMap<ImmutablePair<Integer, Integer>, Double> compute() {
            if (this.lenght < this.chunkSize) {
                return this.computeDirect();
            }

            System.out.println("+++++++++++++++ Starting comparator by NAMES +++++++++++++++");

            // split further
            int split = this.lenght / 2;

            FullRecordsCompareByNameTask[] tasks = {new FullRecordsCompareByNameTask(this.dataMap, this.nerMatrixByNames, sortedNamesNerKeys, this.start, split, this.chunkSize),
                    new FullRecordsCompareByNameTask(this.dataMap, this.nerMatrixByNames, sortedNamesNerKeys, this.start + split, this.lenght - split, this.chunkSize)};
            Collection<FullRecordsCompareByNameTask> tasksResults = invokeAll(Arrays.asList(tasks));

            // merge
            TreeMap<ImmutablePair<Integer, Integer>, Double> dataSet = new TreeMap<>(new NerKeysComparator());
            for (FullRecordsCompareByNameTask task : tasksResults) {
                if (task.isDone()) {
                    dataSet.putAll(task.getRawResult());
                } else {
                    System.out.printf("Failed to get task results, reason: %s\n", task.getException());
                }
            }
            return dataSet;
        }

        private TreeMap<ImmutablePair<Integer, Integer>, Double> computeDirect() {
            TreeMap<ImmutablePair<Integer, Integer>, Double> dataSet = new TreeMap<>(new NerKeysComparator());
            int to = this.start + this.lenght;
            List<Object> firstRecord = null, secRecord;
            int lastLeftId = -1;
            for (int i = this.start; i < to; i++) {
                ImmutablePair<Integer, Integer> key = this.sortedNamesNerKeys.get(i);
                if (lastLeftId != key.left) {
                    // its often that first record has multiple matches
                    firstRecord = this.dataMap.get(key.left);
                    lastLeftId = key.left;
                }
                secRecord = this.dataMap.get(key.right);
                double res = this.compareRecords(firstRecord, secRecord, key);
                if (res > fullThreshold) {
                    dataSet.put(key, res);//TODO 1.0);
                    System.out.printf("%s | %s | %s | %s \n%s | %s | %s | %s\n++++++++++++++++++++++++++++++\n",
                            firstRecord.get(ID_INDEX), firstRecord.get(NAME_INDEX), firstRecord.get(ADDR_INDEX), firstRecord.get(TAXONOM_INDEX),
                            secRecord.get(ID_INDEX), secRecord.get(NAME_INDEX), secRecord.get(ADDR_INDEX), secRecord.get(TAXONOM_INDEX));
                } /*else {
                    logger.info(String.format("%s | %s | %s | %s \n%s | %s | %s | %s\n++++++++++++++++++++++++++++++\n",
                            firstRecord.get(ID_INDEX), firstRecord.get(NAME_INDEX), firstRecord.get(ADDR_INDEX), firstRecord.get(TAXONOM_INDEX),
                            secRecord.get(ID_INDEX), secRecord.get(NAME_INDEX), secRecord.get(ADDR_INDEX), secRecord.get(TAXONOM_INDEX)));
                }*/
            }

            return dataSet;
        }

        private double compareRecords(List<Object> firstRecord, List<Object> secRecord, ImmutablePair<Integer, Integer> nerKey) {
            double namesProb = this.nerMatrixByNames.get(nerKey);
            String firstAddr = (String) firstRecord.get(ADDR_INDEX);
            String secAddr = (String) secRecord.get(ADDR_INDEX);
            double addrProb = USAddressParser.compare(firstAddr, secAddr, distance);

            if (namesProb >= 1.0 && addrProb >= 1.0) {
                // no need to consider further - name and address the same
                return 1.0;
            } else if (addrProb < addrThreshold) {
                // completely different address
                addrProb = 0;
            }
            // compare taxonomies

            String firstTaxon = (String) firstRecord.get(TAXONOM_INDEX);
            String secTaxon = (String) secRecord.get(TAXONOM_INDEX);
            double taxonomProb = compareTaxonomies(firstTaxon, secTaxon);

            if (addrProb >= 1.0 && taxonomProb >= 1.0) {
                return 1.0;
            }


            double finalProb = (namesProb + addrProb + taxonomProb) / 3.0;
            if (finalProb >= 1.0) {
                finalProb = 1.0;
            }
            return finalProb;
        }
    }

    static double compareTaxonomies(String firstTaxon, String secTaxon) {
        if (firstTaxon.equalsIgnoreCase(secTaxon)) {
            // complete match
            return 1.0;
        }
        String[] fTokens = firstTaxon.split(" |,");
        String[] sTokens = secTaxon.split(" |,");
        double taxonomProb = 0;
        for (String fStr : fTokens) {
            fStr = filterNoise(fStr);
            if (fStr.length() < 3) {
                continue;
            }
            for (String sStr : sTokens) {
                sStr = filterNoise(sStr);
                if (sStr.length() < 3) {
                    continue;
                }
                double proximity = distance.proximity(fStr, sStr);
                if (proximity > taxonomProb) {
                    taxonomProb = proximity;
                }
                if (taxonomProb >= 1.0) {
                    // no need to go further
                    break;
                }
            }
            if (taxonomProb >= 1.0) {
                // no need to go further
                break;
            }
        }

        return taxonomProb;
    }

    static String filterNoise(String str) {
        str = str.replace("'", "");
        str = str.replace("&", " ");
        return str;
    }

    // Task to recursively compare records by name fields using Fork-Join thread
    class FullRecordsCompareByAddressTask extends RecursiveTask<TreeMap<ImmutablePair<Integer, Integer>, Double>> {
        // The RAW records
        private final TreeMap<Integer, List<Object>> dataMap;
        // The sorted NER keys
        private final ArrayList<ImmutablePair<Integer, Integer>> sortedAddrNerKeys;
        // The NER results matrix
        private final Map<ImmutablePair<Integer, Integer>, Double> nerMatrixByAddr;

        // The start index (inclusive)
        private final int start;
        // The length of data train to process
        private final int lenght;
        // The minimal size of one chunk
        private final int chunkSize;

        FullRecordsCompareByAddressTask(TreeMap<Integer, List<Object>> dataMap, Map<ImmutablePair<Integer, Integer>, Double> nerMatrix,
                                        ArrayList<ImmutablePair<Integer, Integer>> sortedNerKeys, int start, int lenght, int chunkSize) {
            this.dataMap = dataMap;
            this.nerMatrixByAddr = nerMatrix;
            this.sortedAddrNerKeys = sortedNerKeys;
            this.start = start;
            this.lenght = lenght;
            this.chunkSize = chunkSize;

        }

        @Override
        protected TreeMap<ImmutablePair<Integer, Integer>, Double> compute() {
            if (this.lenght < this.chunkSize) {
                return this.computeDirect();
            }

            System.out.println("+++++++++++++++ Starting comparator by ADDRESS +++++++++++++++");

            // split further
            int split = this.lenght / 2;

            FullRecordsCompareByAddressTask[] tasks = {new FullRecordsCompareByAddressTask(this.dataMap, this.nerMatrixByAddr, sortedAddrNerKeys, this.start, split, this.chunkSize),
                    new FullRecordsCompareByAddressTask(this.dataMap, this.nerMatrixByAddr, sortedAddrNerKeys, this.start + split, this.lenght - split, this.chunkSize)};
            Collection<FullRecordsCompareByAddressTask> tasksResults = invokeAll(Arrays.asList(tasks));

            // merge
            TreeMap<ImmutablePair<Integer, Integer>, Double> dataSet = new TreeMap<>(new NerKeysComparator());
            for (FullRecordsCompareByAddressTask task : tasksResults) {
                if (task.isDone()) {
                    dataSet.putAll(task.getRawResult());
                } else {
                    System.out.printf("Failed to get task results, reason: %s\n", task.getException());
                }
            }
            return dataSet;
        }

        private TreeMap<ImmutablePair<Integer, Integer>, Double> computeDirect() {
            TreeMap<ImmutablePair<Integer, Integer>, Double> dataSet = new TreeMap<>(new NerKeysComparator());
            int to = this.start + this.lenght;
            List<Object> firstRecord = null, secRecord;
            int lastLeftId = -1;
            for (int i = this.start; i < to; i++) {
                ImmutablePair<Integer, Integer> key = this.sortedAddrNerKeys.get(i);
                if (lastLeftId != key.left) {
                    // its often that first record has multiple matches
                    firstRecord = this.dataMap.get(key.left);
                    lastLeftId = key.left;
                }
                secRecord = this.dataMap.get(key.right);
                String firstTaxon = (String) firstRecord.get(TAXONOM_INDEX);
                String secTaxon = (String) secRecord.get(TAXONOM_INDEX);
                double taxonomProb = compareTaxonomies(firstTaxon, secTaxon);
                double addrProb = this.nerMatrixByAddr.get(key);

                if (addrProb >= 1.0 && taxonomProb >= 1.0) {
                    dataSet.put(key, 1.0);
                    System.out.printf("%s | %s | %s | %s \n%s | %s | %s | %s\n++++++++++++++++++++++++++++++\n",
                            firstRecord.get(ID_INDEX), firstRecord.get(NAME_INDEX), firstRecord.get(ADDR_INDEX), firstRecord.get(TAXONOM_INDEX),
                            secRecord.get(ID_INDEX), secRecord.get(NAME_INDEX), secRecord.get(ADDR_INDEX), secRecord.get(TAXONOM_INDEX));
                }/* else {
                    System.out.printf("Excluded: %s | %s | %s <::> %s | %s | %s\n",
                            firstRecord.get(NAME_INDEX), firstRecord.get(ADDR_INDEX), firstRecord.get(TAXONOM_INDEX),
                            secRecord.get(NAME_INDEX), secRecord.get(ADDR_INDEX), secRecord.get(TAXONOM_INDEX));
                }*/
            }

            return dataSet;
        }
    }

    // The comparator to order NER keys in ascending order by 1st record ID and that by 2nd record ID
    class NerKeysComparator implements Comparator<ImmutablePair<Integer, Integer>> {

        @Override
        public int compare(ImmutablePair<Integer, Integer> o1, ImmutablePair<Integer, Integer> o2) {
            int res = o1.left.compareTo(o2.left);
            if (res == 0) {
                res = o1.right.compareTo(o2.right);
            }
            return res;
        }
    }

    // Comparator to sort resulting records in ascending order by first record ID and than by second record ID
    class RecordsResultComparator implements Comparator<List<Object>> {

        @Override
        public int compare(List<Object> o1, List<Object> o2) {
            Integer firstRecID1 = ((Double) o1.get(0)).intValue();
            Integer firstRecID2 = ((Double) o2.get(0)).intValue();
            int res = firstRecID1.compareTo(firstRecID2);
            if (res == 0) {
                // compare by second record ID
                Integer secondRecID1 = ((Double) o1.get(1)).intValue();
                Integer secondRecID2 = ((Double) o2.get(1)).intValue();
                res = secondRecID1.compareTo(secondRecID2);
            }

            return res;
        }
    }
}
