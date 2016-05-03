package ua.nologin.mdm;

import com.aliasi.spell.JaroWinklerDistance;
import org.apache.commons.lang3.tuple.ImmutablePair;
import ua.nologin.mdm.address.USAddress;
import ua.nologin.mdm.address.USAddressParser;

import java.io.File;
import java.io.IOException;
import java.text.Collator;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

import static ua.nologin.mdm.Defines.ADDR_INDEX;
import static ua.nologin.mdm.Defines.ID_INDEX;

/**
 * Created by Iaroslav Omelianenko on 8/17/15.
 */
public class USAddressComparator {

    private JaroWinklerDistance distance;

    // The minimal similarity threshold
    private double threshold;

    public USAddressComparator(double threshold) {
        this.threshold = threshold;

        this.distance = JaroWinklerDistance.JARO_DISTANCE;
    }

    public void compareAddressFields(File input, File results) throws IOException {
        List<List<Object>> dataSet = UtilsIO.loadCSVAsDataSet(input);
        // filter address lines from noise
        String addr;
        for (List<Object> row : dataSet) {
            addr = USAddressParser.filterNoise((String) row.get(ADDR_INDEX));
            row.set(ADDR_INDEX, addr);
        }

        Map<ImmutablePair<Integer, Integer>, Double> matrix = this.compareAddressFieldsOrdered(dataSet);// this.compareAddressFieldsBruteForce(dataSet);

        System.out.printf("Saving processed records to: %s\n", results.getAbsolutePath());
        // save data set
        UtilsIO.saveMatrix(matrix, results);
    }

    public Map<ImmutablePair<Integer, Integer>, Double> compareAddressFieldsBruteForce(List<List<Object>> dataSet) {
        int splitFactor = 20;
        ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix = new ConcurrentHashMap<>(dataSet.size(), 0.75f, splitFactor);

        // do processing in parallel
        int size = dataSet.size();
        BruteForceCompareAddressAction ca = new BruteForceCompareAddressAction(matrix, dataSet, 0, size, size / splitFactor);
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(ca);

        System.out.printf("+++++++++++++++++++++++++++++++++\nFound: %d duplicate address records\n", matrix.size());

        return matrix;
    }

    public Map<ImmutablePair<Integer, Integer>, Double> compareAddressFieldsOrdered(List<List<Object>> dataSet) {
        // order records ascending
        Collections.sort(dataSet, new SortComparator());

//        int splitFactor = 10;
//        ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix = new ConcurrentHashMap<>(dataSet.size(), 0.75f, splitFactor);

        // do processing in parallel
        int size = dataSet.size();
//        CalcDistanceOrderedSetAction ca = new CalcDistanceOrderedSetAction(matrix, dataSet, 0, size);
        CalcDistanceOrderedTask ct = new CalcDistanceOrderedTask(dataSet, 0, size);
        ForkJoinPool pool = new ForkJoinPool();
        HashMap<ImmutablePair<Integer, Integer>, Double> matrix = pool.invoke(ct);

        System.out.printf("+++++++++++++++++++++++++++++++++\nFound: %d duplicate address records\n", matrix.size());

        return matrix;
    }

    class CalcDistanceOrderedTask extends RecursiveTask<HashMap<ImmutablePair<Integer, Integer>, Double>> {
        // The results holder
        private HashMap<ImmutablePair<Integer, Integer>, Double> matrix;
        // The input data set
        private final List<List<Object>> dataSet;
        // The start index (inclusive)
        private final int start;
        // The length of data train to process
        private final int lenght;

        private char letter;
        private char secLetter;

        public CalcDistanceOrderedTask(List<List<Object>> dataSet, int from, int lenght) {
            this.dataSet = dataSet;
            this.start = from;
            this.lenght = lenght;
        }

        @Override
        protected HashMap<ImmutablePair<Integer, Integer>, Double> compute() {
            int dsSize = this.dataSet.size();
            if (dsSize > lenght) {
                this.matrix = new HashMap<>(lenght, 0.75f);
                // do processing
                return this.computeDirect();
            }

            // split tasks by city name
            return this.splitTasks();
        }

        private HashMap<ImmutablePair<Integer, Integer>, Double> splitTasks() {
            int dsSize = this.dataSet.size();
            ArrayList<CalcDistanceOrderedTask> tasks = new ArrayList<>();
            char letter = 'A';// start letter
            int dsStartIndex = 0;
            for (int i = 0; i < dsSize; i++) {
                List<Object> row = dataSet.get(i);
                String addr = (String) row.get(ADDR_INDEX);
                USAddress usAddress = USAddressParser.parse(addr);
                char first = ' ';
                if (usAddress.getCity().length() > 0) {
                    first = usAddress.getCity().charAt(0);
                }
                if (dsStartIndex == 0 && first == letter) {
                    // the very first block
                    CalcDistanceOrderedTask task = new CalcDistanceOrderedTask(dataSet, dsStartIndex, i - dsStartIndex);
                    tasks.add(task);
                    task.letter = first;
                    dsStartIndex = i;
                    letter = first;
                } else if (i == dsSize - 1) {
                    // the last block
                    CalcDistanceOrderedTask task = new CalcDistanceOrderedTask(dataSet, dsStartIndex, i - dsStartIndex);
                    tasks.add(task);
                } else if (first != letter && dsStartIndex > 0) {
                    // the next block start
                    CalcDistanceOrderedTask task = new CalcDistanceOrderedTask(dataSet, dsStartIndex, i - dsStartIndex);
                    tasks.add(task);
                    task.letter = letter;
                    dsStartIndex = i;
                    letter = first;
                }
            }

            // start all tasks
            this.matrix = new HashMap<>(lenght, 0.75f);
            Collection<CalcDistanceOrderedTask> resTasks = invokeAll(tasks);
            this.merge(resTasks);
            return this.matrix;
        }

        private void merge(Collection<CalcDistanceOrderedTask> resTasks) {
            for (CalcDistanceOrderedTask task : resTasks) {
                this.matrix.putAll(task.getRawResult());
                task.setRawResult(null);// release resources
            }
        }

        private HashMap<ImmutablePair<Integer, Integer>, Double> computeDirect() {
            int to = this.start + this.lenght;
            for (int i = this.start; i < to - 1; i++) {
                List<Object> row = this.dataSet.get(i);
                this.calcDistance((String) row.get(ADDR_INDEX), i, to);
            }
            System.out.printf("################ Complete for letter: [%c] from: %d, to: %d\n", this.letter, this.start, to);

            return this.matrix;
        }

        private void calcDistance(String firstAddrLine, int from, int to) {
            System.out.printf("Proceed with: %s, start: %d, to: %d\n", firstAddrLine, from, to);
            USAddress first = USAddressParser.parse(firstAddrLine);

            int fromId = ((Double)dataSet.get(from).get(ID_INDEX)).intValue();
            String secAddrLine;
            List<Object>row;
            for (int i = from + 1; i < to; i++) {
                row = dataSet.get(i);
                secAddrLine = (String) row.get(ADDR_INDEX);
                USAddress second = USAddressParser.parse(secAddrLine);

                int toId = ((Double) row.get(ID_INDEX)).intValue();
                double proximity = USAddressParser.compare(first, second, distance);
                if (proximity > threshold) {
                    if (fromId > toId) {
                        // rearrange
                        int tmp = fromId;
                        fromId = toId;
                        toId = tmp;
                    }
                    this.matrix.put(new ImmutablePair(fromId, toId), proximity);
//                    System.out.printf("%c - %d : %d : %.2f <> %s | %s\n", letter, fromId, toId, proximity, firstAddrLine, secAddrLine);
                }
            }
        }
    }

    class CalcDistanceOrderedSetAction extends RecursiveAction {
        // The results holder
        private ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix;
        // The input data set
        private final List<List<Object>> dataSet;
        // The start index (inclusive)
        private final int start;
        // The length of data train to process
        private final int lenght;

        private HashMap<String, USAddress> addrCache;

        private char letter;

        public CalcDistanceOrderedSetAction(ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix, List<List<Object>> dataSet, int from, int lenght) {
            this.matrix = matrix;
            this.dataSet = dataSet;
            this.start = from;
            this.lenght = lenght;
        }

        @Override
        protected void compute() {
            int dsSize = this.dataSet.size();
            if (dsSize > lenght) {
                // init cache
                this.addrCache = new HashMap<>(lenght, 0.75f);

                // do processing
                this.computeDirect();

                // release resources
                addrCache = null;

                return;
            }

            // split tasks by alphabet
            ArrayList<CalcDistanceOrderedSetAction> tasks = new ArrayList<>();
            char letter = '0';// start letter
            int dsStartIndex = 0;
            List<Object> row;
            String addr;
            for (int i = 0; i < dsSize; i++) {
                row = dataSet.get(i);
                addr = (String) row.get(ADDR_INDEX);
                char first = addr.charAt(0);
                if (dsStartIndex == 0 && first == letter) {
                    // the very first block
                    CalcDistanceOrderedSetAction task = new CalcDistanceOrderedSetAction(matrix, dataSet, dsStartIndex, i - dsStartIndex);
                    tasks.add(task);
                    task.letter = first;
                    dsStartIndex = i;
                    letter = first;
                } else if (i == dsSize - 1) {
                    // the last block
                    CalcDistanceOrderedSetAction task = new CalcDistanceOrderedSetAction(matrix, dataSet, dsStartIndex, i - dsStartIndex);
                    tasks.add(task);
                } else if (first != letter && dsStartIndex > 0) {
                    // the next block start
                    CalcDistanceOrderedSetAction task = new CalcDistanceOrderedSetAction(matrix, dataSet, dsStartIndex, i - dsStartIndex);
                    tasks.add(task);
                    task.letter = letter;
                    dsStartIndex = i;
                    letter = first;
                }
            }
            // start all tasks
            invokeAll(tasks);
        }

        private void computeDirect() {
            int to = this.start + this.lenght;
            for (int i = this.start; i < to - 1; i++) {
                List<Object> row = this.dataSet.get(i);
                this.calcDistance((String) row.get(ADDR_INDEX), i, to);
            }
            System.out.printf("Complete for letter: [%c] from: %d, to: %d\n", this.letter, this.start, to);
        }

        private void calcDistance(String firstAddrLine, int from, int to) {
            List<Object>row;
            int fromId = ((Double)dataSet.get(from).get(ID_INDEX)).intValue();
            USAddress first = this.addrCache.get(firstAddrLine);
            if (first == null) {
                first = USAddressParser.parse(firstAddrLine);
                this.addrCache.put(firstAddrLine, first);
            }
            String secAddrLine;
            for (int i = from + 1; i < to; i++) {
                row = dataSet.get(i);
                secAddrLine = (String) row.get(ADDR_INDEX);
                USAddress second = this.addrCache.get(secAddrLine);
                if (second == null) {
                    second = USAddressParser.parse(secAddrLine);
                    this.addrCache.put(secAddrLine, second);
                }

                int toId = ((Double) row.get(ID_INDEX)).intValue();
                double proximity = USAddressParser.compare(first, second, distance);
                if (proximity > threshold) {
                    if (fromId > toId) {
                        // rearrange
                        int tmp = fromId;
                        fromId = toId;
                        toId = tmp;
                    }
                    matrix.put(new ImmutablePair(fromId, toId), proximity);
//                    System.out.printf("%c - %d : %d : %.2f <> %s | %s\n", letter, fromId, toId, proximity, firstAddrLine, secAddrLine);
                }
            }
        }
    }

    // The action to compare address fields in parallel
    class BruteForceCompareAddressAction extends RecursiveAction {
        // The results holder
        private ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix;
        // The input data set
        private final List<List<Object>> dataSet;
        // The start index (inclusive)
        private final int start;
        // The length of data train to process
        private final int lenght;
        // The minimal size of one chunk
        private final int chunkSize;

        BruteForceCompareAddressAction(ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix, List<List<Object>> dataSet, int from, int lenght, int chunkSize) {
            this.matrix = matrix;
            this.dataSet = dataSet;
            this.start = from;
            this.lenght = lenght;
            this.chunkSize = chunkSize;
        }

        @Override
        protected void compute() {
            if (this.lenght < this.chunkSize) {
                this.computeDirect();
                return;
            }
            // split further
            int split = this.lenght / 2;

            invokeAll(new BruteForceCompareAddressAction(this.matrix, this.dataSet, this.start, split, this.chunkSize),
                    new BruteForceCompareAddressAction(this.matrix, this.dataSet, this.start + split, this.lenght - split, this.chunkSize));
        }

        private void computeDirect() {
            int to = this.start + this.lenght;
            for (int i = this.start; i < to; i++) {
                List<Object>row = this.dataSet.get(i);
                this.calcDistance((String) row.get(ADDR_INDEX), i);
            }
        }

        private void calcDistance(String firstAddrLine, int from) {
            int size = this.dataSet.size();
            List<Object>row;
            int fromId = ((Double)dataSet.get(from).get(ID_INDEX)).intValue();
            for (int i = from + 1; i < size; i++) {
                row = dataSet.get(i);
                String secAddrLine = (String) row.get(ADDR_INDEX);
                int toId = ((Double) row.get(ID_INDEX)).intValue();
                double proximity = USAddressParser.compare(firstAddrLine, secAddrLine, distance);
                if (proximity > threshold) {
                    if (fromId > toId) {
                        // rearrange
                        int tmp = fromId;
                        fromId = toId;
                        toId = tmp;
                    }
                    matrix.put(new ImmutablePair(fromId, toId), proximity);
                    System.out.printf("%d : %d : %.2f <> %s | %s\n", fromId, toId, proximity, firstAddrLine, secAddrLine);
                }
            }
        }
    }

    /*
    Sort order comparator to sort records in natural order first by name field and than by ID field
     */
    class SortComparator implements Comparator<List<Object>> {
        private Collator collator;

        public SortComparator() {
            this.collator = Collator.getInstance(Locale.US);
        }

        @Override
        public int compare(List<Object> o1, List<Object> o2) {
            String firstAddr = (String) o1.get(ADDR_INDEX);
            USAddress first = USAddressParser.parse(firstAddr);
            String secAddr = (String) o2.get(ADDR_INDEX);
            USAddress second = USAddressParser.parse(secAddr);

            int res = this.collator.compare(first.getCity(), second.getCity());
            if (res == 0) {
                // compare by ID
                Integer firstID = ((Double) o1.get(ID_INDEX)).intValue();
                Integer secID = ((Double) o2.get(ID_INDEX)).intValue();
                res = firstID.compareTo(secID);
            }

            return res;
        }
    }
}
