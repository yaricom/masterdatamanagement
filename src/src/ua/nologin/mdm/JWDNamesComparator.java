package ua.nologin.mdm;

import com.aliasi.spell.JaroWinklerDistance;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static ua.nologin.mdm.Defines.ID_INDEX;
import static ua.nologin.mdm.Defines.NAME_INDEX;

/**
 * The name fields comparator based on Jaro-Winkler distance.
 *
 * Created by Iaroslav Omelianenko on 8/14/15.
 */
public class JWDNamesComparator {

    private JaroWinklerDistance jwd;
    // The minimal similarity threshold
    private double threshold;

    public JWDNamesComparator(double threshold) {
        this.threshold = threshold;
        this.jwd = JaroWinklerDistance.JARO_WINKLER_DISTANCE;
    }

    /**
     * Method to compare names in the data set loaded from specified input file and to store results as matrix.
     *
     * @param input   the input file with CSV data set
     * @param results the output file to store resulting matrix
     */
    public void compareNames(File input, File results) throws IOException {
        List<List<Object>> dataSet = UtilsIO.loadCSVAsDataSet(input);

        ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix = this.compareNamesOrderedSet(dataSet);//this.compareNamesBruteForce(dataSet);

        System.out.printf("Saving processed records to: %s\n", results.getAbsolutePath());
        // save data set
        UtilsIO.saveMatrix(matrix, results);
    }

    /**
     * Compare names by brute force iteration over all records against each record. Takes too long time, but it's more accurate.
     *
     * @param dataSet the data set to proceed.
     * @return the processed matrix.
     */
    public ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> compareNamesBruteForce(List<List<Object>> dataSet) {
        ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix = new ConcurrentHashMap<>(dataSet.size(), 0.75f);

        // do processing in parallel
        int size = dataSet.size();
        CalcDistanceBruteForceAction da = new CalcDistanceBruteForceAction(matrix, dataSet, 0, size, size / 4);
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(da);

        System.out.printf("+++++++++++++++++++++++++++++++++\nFound: %d duplicate names records\n", matrix.size());

        return matrix;
    }

    /**
     * Compare names by iteration through ordered by name records against each record. Much faster than brute force.
     *
     * @param dataSet the data set to proceed.
     * @return the processed matrix.
     */
    public ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> compareNamesOrderedSet(List<List<Object>> dataSet) {
        ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix = new ConcurrentHashMap<>(dataSet.size(), 0.75f, 26);

        // do processing in parallel
        int size = dataSet.size();
        CalcDistanceOrderedSetAction da = new CalcDistanceOrderedSetAction(matrix, dataSet, 0, size);
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(da);

        System.out.printf("+++++++++++++++++++++++++++++++++\nFound: %d duplicate names records\n", matrix.size());

        return matrix;
    }

    // The task to recursively calculate distance in parallel assuming that input set ordered naturally by name
    class CalcDistanceOrderedSetAction extends RecursiveAction {
        // The results holder
        private ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix;
        // The input data set
        private final List<List<Object>> dataSet;
        // The start index (inclusive)
        private final int start;
        // The length of data train to process
        private final int lenght;

        private char letter;


        CalcDistanceOrderedSetAction(ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix, List<List<Object>> dataSet, int from, int lenght) {
            this.matrix = matrix;
            this.dataSet = dataSet;
            this.start = from;
            this.lenght = lenght;
        }

        @Override
        protected void compute() {
            int dsSize = this.dataSet.size();
            if (dsSize > lenght) {
                this.computeDirect();
                return;
            }

            // split tasks by alphabet
            ArrayList<CalcDistanceOrderedSetAction> tasks = new ArrayList<>();
            char[] alphabet = "abcdefghijklmnopqrstuvwxyz_".toUpperCase().toCharArray();
            int alphabetIndex = 0;
            char letter = alphabet[alphabetIndex++];// start letter 'A'
            int dsStartIndex = 0;
            List<Object> row;
            String name;
            for (int i = 0; i < dsSize; i++) {
                row = dataSet.get(i);
                name = (String) row.get(NAME_INDEX);
                char first = name.charAt(0);
                if (first == letter) {
                    // store block and move to the next
                    CalcDistanceOrderedSetAction task = new CalcDistanceOrderedSetAction(matrix, dataSet, dsStartIndex, i - dsStartIndex);
                    tasks.add(task);
                    task.letter = letter;
                    dsStartIndex = i;
                    if (alphabetIndex < alphabet.length) {
                        letter = alphabet[alphabetIndex++];
                    }
                } else if (i == dsSize - 1) {
                    // the last block for 'Z'
                    CalcDistanceOrderedSetAction task = new CalcDistanceOrderedSetAction(matrix, dataSet, dsStartIndex, i - dsStartIndex);
                    tasks.add(task);
                }
            }
            // start all tasks
            invokeAll(tasks);
        }

        private void computeDirect() {
            int to = this.start + this.lenght;
            for (int i = this.start; i < to - 1; i++) {
                List<Object> row = this.dataSet.get(i);
                this.calcDistance((String) row.get(NAME_INDEX), i, to);
            }
            System.out.printf("Complete for letter: [%c] from: %d, to: %d\n", this.letter, this.start, to);
        }

        private void calcDistance(String name, int from, int to) {
            System.out.printf("Proceed with: %s, start: %d, to: %d\n", name, from, to);
            List<Object> row;
            int fromId = ((Double) dataSet.get(from).get(ID_INDEX)).intValue();
            for (int i = from + 1; i < to; i++) {
                row = dataSet.get(i);
                String secName = (String) row.get(NAME_INDEX);
                int toId = ((Double) row.get(ID_INDEX)).intValue();
                double proximity = jwd.proximity(name, secName);
                if (proximity >= threshold) {
                    if (fromId > toId) {
                        // rearrange
                        int tmp = fromId;
                        fromId = toId;
                        toId = tmp;
                    }
                    matrix.put(new ImmutablePair(fromId, toId), proximity);
                    System.out.printf("[%c] %d : %d : %.2f <> %s | %s\n", this.letter, fromId, toId, proximity, name, secName);
                }
            }
        }
    }

    // The task to recursively calculate distance in parallel using brute force approach
    class CalcDistanceBruteForceAction extends RecursiveAction {
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

        CalcDistanceBruteForceAction(ConcurrentHashMap<ImmutablePair<Integer, Integer>, Double> matrix, List<List<Object>> dataSet, int from, int lenght, int chunkSize) {
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

            invokeAll(new CalcDistanceBruteForceAction(this.matrix, this.dataSet, this.start, split, this.chunkSize),
                    new CalcDistanceBruteForceAction(this.matrix, this.dataSet, this.start + split, this.lenght - split, this.chunkSize));

        }

        private void computeDirect() {
            int to = this.start + this.lenght;
            for (int i = this.start; i < to; i++) {
                List<Object> row = this.dataSet.get(i);
                this.calcDistance((String) row.get(NAME_INDEX), i);
            }
        }

        private void calcDistance(String name, int from) {
            int size = this.dataSet.size();
            List<Object> row;
            int fromId = ((Double) dataSet.get(from).get(ID_INDEX)).intValue();
            for (int i = from + 1; i < size; i++) {
                row = dataSet.get(i);
                String secName = (String) row.get(NAME_INDEX);
                int toId = ((Double) row.get(ID_INDEX)).intValue();
                double proximity = jwd.proximity(name, secName);
                if (proximity >= threshold) {
                    if (fromId > toId) {
                        // rearrange
                        int tmp = fromId;
                        fromId = toId;
                        toId = tmp;
                    }
                    matrix.put(new ImmutablePair(fromId, toId), proximity);
                    System.out.printf("%d : %d <> %s | %s\n", fromId, toId, name, secName);
                }
            }
        }
    }
}
