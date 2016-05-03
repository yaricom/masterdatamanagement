package ua.nologin.mdm;/**
 * Created by yaric on 8/19/15.
 */

import org.apache.commons.lang3.tuple.ImmutablePair;
import ua.nologin.mdm.converters.CSVLoader;
import ua.nologin.mdm.converters.CSVSaver;

import java.io.*;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

/**
 * The IO utils to save/load data to/from disk
 *
 * @author Iaroslav Omelianenko
 */
public class UtilsIO {

    public static void saveMatrix(Map<ImmutablePair<Integer, Integer>, Double> matrix, File file) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file))) {
            out.writeObject(matrix);
        }
    }

    public static Map<ImmutablePair<Integer, Integer>, Double> loadMatrix(File file) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(file))) {
            Map<ImmutablePair<Integer, Integer>, Double> matrix = (Map<ImmutablePair<Integer, Integer>, Double>) in.readObject();
            System.out.printf("Loaded matrix with %d rows\n", matrix.size());
            return matrix;
        }
    }

    public static List<List<Object>> loadCSVAsDataSet(File input) throws IOException {
        List<List<Object>> dataSet;
        try (CSVLoader loader = new CSVLoader()) {
            loader.setSource(input);
            dataSet = loader.getDataSet();
        }
        System.out.printf("Loaded data set with %d rows\n", dataSet.size());
        return dataSet;
    }

    public static void saveCSVDataSet(List<List<Object>> dataSet, CSVSaver.FieldType[] types, File file) throws IOException {
        try (CSVSaver saver = new CSVSaver()) {
            saver.setFile(file);
            saver.write(dataSet, types);
        }
    }
}
