package ua.nologin.mdm;/**
 * Created by yaric on 8/14/15.
 */

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import ua.nologin.mdm.converters.CSVSaver;

import java.io.File;
import java.io.IOException;
import java.text.Collator;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import static ua.nologin.mdm.Defines.ID_INDEX;
import static ua.nologin.mdm.Defines.NAME_INDEX;

/**
 * The Named Entity Recognizer based on Stanford NER library. This NER is aimed to extract people names from NAME field of data set records.
 *
 * @author Iaroslav Omelianenko
 */
public class StanfordNERProcessor {

    private String serializedClassifier;
    private AbstractSequenceClassifier<CoreLabel> classifier;

    // the list of regexp to be filtered
    private String[] badLiter;
    private String[] badReg;

    public StanfordNERProcessor(String serializedClassifier) throws IOException, ClassNotFoundException {
        this.serializedClassifier = serializedClassifier;

        this.classifier = CRFClassifier.getClassifier(serializedClassifier);
        badLiter = new String[]{
                "MR.", "MR", "M.R", "MRS.", "MRS", "MSR.", "M.SR", "MSSI", "MS.", "M.S", "M.S.", ".SM",
                "DR.", "D.R", ".RD", "RD.", ".RM", "O.D.", "O. D.", "D.M.", "M. D.", "M.D.", "MD",
                "MA.", "MA", "M.A.", "M.", "CCC-SLP", "CCC/LSP", "CCC/SLP", "S.L.P.", "ST", "ST.", ".TS"
        };
        badReg = new String[badLiter.length * 2];
        int i = 0;
        for (String str : badLiter) {
            badReg[i * 2] = "^\\s*" + str + "\\s";
            badReg[i * 2 + 1] = "\\s" + str + "\\s*$";
            i++;
        }
    }

    public void processNames(File input, File output) throws IOException {
        List<List<Object>> dataSet = UtilsIO.loadCSVAsDataSet(input);

        // process loaded data set
        this.processNames(dataSet);

        // sort data set
        dataSet.sort(new SortComparator());

        System.out.printf("Saving processed records to: %s\n", output.getAbsolutePath());
        // save data set
        CSVSaver.FieldType[] types = {CSVSaver.FieldType.INT, CSVSaver.FieldType.STRING,
                CSVSaver.FieldType.STRING, CSVSaver.FieldType.STRING};
        UtilsIO.saveCSVDataSet(dataSet, types, output);
    }

    public void processNames(List<List<Object>> dataSet) {
        for (List<Object> row : dataSet) {
            String origName = (String) row.get(NAME_INDEX);
            String nameNE = extractNE(origName, 2);// at least 2 words per name
            nameNE = this.filterName(nameNE);
            if (nameNE.length() > 5) {
                row.set(NAME_INDEX, nameNE);
            } else {
                // set original name due to error of NE recognition
                nameNE = this.filterName(origName);
                System.out.printf("Failed to NER original name: [%s]. Using filtered original name: [%s]!\n", origName, nameNE);
                row.set(NAME_INDEX, nameNE);
            }
//            System.out.printf("%s\t-->\t%s\n", name, nameNE);
        }
    }

    /**
     * Preceed with specified text by extracting NE and returning only NE part of input text or input text if failed to find NE.
     *
     * @param text     the input text
     * @param minWords the minimum number of words per NE
     * @return
     */
    private String extractNE(String text, int minWords) {
        StringBuilder buff = new StringBuilder();
        List<List<CoreLabel>> out = classifier.classify(text.trim());
        if (out.size() == 0) {
            // nothing found
            return text;
        }

        int wordsFound = 0;
        for (List<CoreLabel> sentence : out) {
            for (CoreLabel word : sentence) {
                String annotation = word.get(CoreAnnotations.AnswerAnnotation.class);
                if (annotation.equals("PERSON")) {
                    buff.append(word).append(" ");
                    wordsFound++;
                }

            }
        }
        if (wordsFound < minWords) {
            // less words as expected
            return text;
        }

        return buff.toString().trim();
    }

    private String filterName(String name) {
        // remove special prefixes/suffixes
        for (int i = 0; i < badLiter.length; i++) {
            if (name.contains(badLiter[i])) {
                name = name.replaceFirst(badReg[i * 2], "");
                name = name.replaceFirst(badReg[i * 2 + 1], "");
            }
        }

        // remove suffixes
        int index = name.indexOf(",");
        if (index > 8) {
            name = name.substring(0, index);
        }

        // remove noise
        name.replace("\'", "");

        return name.trim();
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
            String firstName = (String) o1.get(NAME_INDEX);
            String secName = (String) o2.get(NAME_INDEX);

            int res = this.collator.compare(firstName, secName);
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
