package ua.nologin.mdm.converters;/**
 * Created by yaric on 8/14/15.
 */

import java.io.*;
import java.util.List;

/**
 * @author Iaroslav Omelianenko
 */
public class CSVSaver implements Closeable {

    public enum FieldType {
        INT,
        DOUBLE,
        STRING
    }
    /**
     * Holds the source of the data set.
     */
    protected File m_outputFile;
    /**
     * The writer.
     */
    private transient PrintWriter m_writer;

    public void write(List<List<Object>> data, FieldType[] fieldTypes) {
        if (data == null) {
            throw new IllegalArgumentException("Data is NULL");
        }
        if (data.size() == 0) {
            return;
        }
        if (data.get(0).size() != fieldTypes.length) {
            throw new IllegalArgumentException("Data set row should have the same number of fields as provided fields types array");
        }

        Object value;
        Double dvalue;
        int rowSize;
        for (List<Object> row : data) {
            rowSize = row.size();
            for (int i = 0; i < rowSize; i++) {
                value = row.get(i);
                switch (fieldTypes[i]) {
                    case STRING:
                        m_writer.printf("\"%s\"", (String)value);
                        break;

                    case INT:
                        dvalue = (Double)value;
                        m_writer.print(dvalue.intValue());
                        break;

                    default:
                        m_writer.print(value.toString());
                        break;
                }
                if (i < rowSize - 1) {
                    m_writer.print(",");
                } else {
                    m_writer.println();
                }
            }
        }
    }

    /**
     * Sets the destination file.
     *
     * @param outputFile the destination file.
     * @throws java.io.IOException throws an IOException if file cannot be set
     */
    public void setFile(File outputFile) throws IOException {
        setDestination(outputFile);
    }

    /**
     * Sets the destination output stream.
     *
     * @param output the output stream.
     * @throws IOException throws an IOException if destination cannot be set
     */
    public void setDestination(OutputStream output) throws IOException {
        m_writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(output)));
    }

    /**
     * Sets the destination file (and directories if necessary).
     *
     * @param file the File
     * @throws IOException always
     */
    private void setDestination(File file) throws IOException {
        boolean success = false;
        m_outputFile = file;
        if (m_outputFile != null) {
            try {
                if (file.exists()) {
                    if (!file.delete())
                        throw new IOException("File already exists.");
                }
                String out = file.getAbsolutePath();
                if (out.lastIndexOf(File.separatorChar) == -1) {
                    success = file.createNewFile();
                } else {
                    String outPath = out.substring(0, out.lastIndexOf(File.separatorChar));
                    File dir = new File(outPath);
                    if (dir.exists())
                        success = file.createNewFile();
                    else {
                        dir.mkdirs();
                        success = file.createNewFile();
                    }
                }
                if (success) {
                    m_outputFile = file;
                    setDestination(new FileOutputStream(m_outputFile));
                }
            } catch (Exception ex) {
                throw new IOException("Cannot create a new output file (Reason: " + ex.toString() + ").");
            } finally {
                if (!success) {
                    System.err.println("Cannot create a new output file. Standard out is used.");
                    m_outputFile = null; //use standard out
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (m_writer != null) {
            m_writer.close();
        }
    }
}
