package ua.nologin.mdm;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.io.IOException;

public class MDMApplication {

    private File inputFile, gtfFile, outputFile, modelFile, model2File;
    private Configuration config;

    public static void main(String[] args) {
        LongOpt[] longopts = new LongOpt[5];
        longopts[0] = new LongOpt("config", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        longopts[1] = new LongOpt("nernames", LongOpt.NO_ARGUMENT, null, 1);
        longopts[2] = new LongOpt("namecompare", LongOpt.NO_ARGUMENT, null, 2);
        longopts[3] = new LongOpt("addrcompare", LongOpt.NO_ARGUMENT, null, 3);
        longopts[4] = new LongOpt("fullcompare", LongOpt.NO_ARGUMENT, null, 4);

        Getopt getopt = new Getopt("mdm", args, "", longopts);
//        getopt.setOpterr(false);
        int c;
        String arg;
        boolean nernames = false;
        boolean runNameComparator = false;
        boolean runAddrComparator = false;
        boolean runFullCompare = false;
        MDMApplication application = null;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case 1:
                    nernames = true;
                    break;

                case 2:
                    runNameComparator = true;
                    break;

                case 3:
                    runAddrComparator = true;
                    break;

                case 4:
                    runFullCompare = true;
                    break;

                case 'c':
                    arg = getopt.getOptarg();
                    if (arg == null) {
                        System.out.println("No configuration file specified");
                        System.exit(1);
                    }
                    // load configuration
                    Parameters params = new Parameters();
                    FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                            new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                                    .configure(params.properties().setFileName(arg));
                    Configuration config = null;
                    try {
                        config = builder.getConfiguration();
                    } catch (ConfigurationException e) {
                        e.printStackTrace();
                        System.out.println("Failed to parse configuration");
                        System.exit(1);
                    }

                    application = new MDMApplication(config);
                    break;

                default:
                    System.out.println("Unknown argument: " + String.valueOf(c));
                    System.exit(1);
            }
        }

        if (application == null) {
            System.out.println("Missed configuration file's location option.");
            System.exit(1);
        }

        if (nernames) {
            application.nerNames();
        } else if (runNameComparator) {
            application.runNameComparator();
        } else if (runAddrComparator) {
            application.runAddressComparator();
        } else if (runFullCompare) {
            application.runFullComparator();
        } else {
            application.predict();
        }
    }

    public MDMApplication(Configuration config) {
        this.config = config;
    }

    private void runFullComparator() {
        this.setInputFile(this.config.getString("full.compare.input.file"));
        this.setOutputFile(this.config.getString("full.compare.output.file"));
        this.setModelFile(this.config.getString("names.compare.output.file"));
        this.setModel2File(this.config.getString("addr.compare.output.file"));
        FullFieldsComparator comparator = new FullFieldsComparator(
                this.config.getDouble("full.compare.probability.full_threshold"),
                this.config.getDouble("full.compare.probability.addr_threshold"));
        try {
//            comparator.compareRecords(this.inputFile, this.modelFile, this.model2File, this.outputFile);
            comparator.compareRecords(this.inputFile, this.modelFile, this.outputFile);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to run full fields comparator!");
            System.exit(1);
        }

    }

    private void runAddressComparator() {
        this.setInputFile(this.config.getString("addr.compare.input.file"));
        this.setOutputFile(this.config.getString("addr.compare.output.file"));
        USAddressComparator comparator = new USAddressComparator(this.config.getDouble("addr.compare.proximity.threshold"));
        try {
            comparator.compareAddressFields(this.inputFile, this.outputFile);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to run address fields comparator!");
            System.exit(1);
        }
    }

    private void runNameComparator() {
        this.setInputFile(this.config.getString("names.compare.input.file"));
        this.setOutputFile(this.config.getString("names.compare.output.file"));
        JWDNamesComparator comparator = new JWDNamesComparator(this.config.getDouble("names.compare.proximity.threshold"));
        try {
            comparator.compareNames(this.inputFile, this.outputFile);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to run names comparator!");
            System.exit(1);
        }
    }

    private void nerNames() {
        this.setInputFile(this.config.getString("names.ner.input.file"));
        this.setOutputFile(this.config.getString("names.ner.output.file"));
        this.setModelFile(this.config.getString("names.ner.classifier.file"));
        try {
            StanfordNERProcessor ner = new StanfordNERProcessor(this.modelFile.getAbsolutePath());
            ner.processNames(this.inputFile, this.outputFile);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to preprocess!");
            System.exit(1);
        }

    }


    private void predict() {

    }

    private void setOutputFile(String arg) {
        outputFile = new File(arg);
        if (!outputFile.getParentFile().exists()) {
            System.out.printf("The output's file parent directory %s is not found", outputFile.getAbsolutePath());
            System.exit(1);
        }
    }

    private void setGTFFile(String arg) {
        gtfFile = new File(arg);
        if (!gtfFile.exists()) {
            System.out.printf("GTF file %s is not found", gtfFile.getAbsolutePath());
            System.exit(1);
        }
    }

    private void setInputFile(String arg) {
        inputFile = new File(arg);
        if (!inputFile.exists()) {
            System.out.printf("Input file %s is not found", inputFile.getAbsolutePath());
            System.exit(1);
        }
    }

    private void setModelFile(String arg) {
        modelFile = new File(arg);
        if (!modelFile.exists()) {
            System.out.printf("Model file %s is not found", modelFile.getAbsolutePath());
            System.exit(1);
        }
    }

    private void setModel2File(String arg) {
        model2File = new File(arg);
        if (!model2File.exists()) {
            System.out.printf("Model file %s is not found", model2File.getAbsolutePath());
            System.exit(1);
        }
    }
}
