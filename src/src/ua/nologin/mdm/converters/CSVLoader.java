package ua.nologin.mdm.converters;/**
 * Created by yaric on 8/13/15.
 */

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Iaroslav Omelianenko
 */
public class CSVLoader implements Closeable{
    /** The reader for the data. */
    protected transient BufferedReader m_sourceReader;
    /** Holds the source of the data set. */
    protected File m_sourceFile;

    /**
     * Resets the Loader object and sets the source of the data set to be the
     * supplied Stream object.
     *
     * @param input the input stream
     * @exception java.io.IOException if an error occurs
     */
    public void setSource(InputStream input) throws IOException {
        m_sourceReader = new BufferedReader(new InputStreamReader(input));
    }

    /**
     * Resets the Loader object and sets the source of the data set to be
     * the supplied File object.
     *
     * @param file 		the source file.
     * @throws IOException 	if an error occurs
     */
    public void setSource(File file) throws IOException {
        m_sourceFile = file;

        if (file == null)
            throw new IOException("Source file object is null!");

        // set the source only if the file exists
        if (file.exists()) {
            setSource(new FileInputStream(file));
        }
    }

    /**
     * Return the full data set. If the structure hasn't yet been determined by a
     * call to getStructure then method should do so before processing the rest of
     * the data set.
     *
     * @return the full data set
     * @exception IOException if there is no source or parsing fails
     */
    public List<List<Object>> getDataSet() throws IOException {
        if ((m_sourceFile == null) && (m_sourceReader == null)) {
            throw new IOException("No source has been specified");
        }
        ArrayList<List<Object>>dataSet = new ArrayList<>();
        ArrayList<Object>current;

        CSVFormat format = CSVFormat.DEFAULT.withSkipHeaderRecord();
        Iterable<CSVRecord> records = format.parse(m_sourceReader);
        for (CSVRecord record : records) {
            int count = record.size();
            current = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                // try to parse as a number
                String val = record.get(i);
                try {
                    current.add(Double.valueOf(val));
                } catch (NumberFormatException e) {
                    // otherwise assume its an enumerated value
                    current.add(new String(val).trim());
                }
            }
            dataSet.add(current);
        }

        return dataSet;
    }

    @Override
    public void close() throws IOException {
        if (m_sourceReader != null) {
            m_sourceReader.close();
        }
    }
}
