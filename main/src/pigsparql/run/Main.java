package pigsparql.run;

import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import pigsparql.pig.Translator;

/**
 * Main Class for program start.
 * Parses the commandline arguments and calls the PigSPARQL translator.
 * 
 * Options:
 * -h, --help                prints the usage help message
 * -d, --delimiter <value>   delimiter used in RDF triples if not whitespace
 * -e, --expand              expand prefixes used in the query
 * -opt, --optimize          turn on SPARQL algebra optimization
 * -i, --input <file>        SPARQL query file to translate
 * -o, --output <file>       Pig output script file
 *
 * @author Alexander Schaetzle
 */
public class Main {

    //Default values if not overwritten by commandline arguments
    //private static String inputFile = "C:\\SVN\\AlexMartin\\Projects\\PigSPARQL\\trunk\\main\\queries\\q1.sparql";
    //private static String outputFile = "C:\\SVN\\AlexMartin\\Projects\\PigSPARQL\\trunk\\main\\queries\\q1.pig";
    private static String inputFile;
    private static String outputFile;
    private static String delimiter = " ";
    private static boolean optimize = false;
    private static boolean expand = false;
    
    // Define a static logger variable so that it references the corresponding Logger instance
    private static Logger logger = Logger.getLogger(Main.class);


    /**
     * Main method invoked on program start.
     * It parses the commandline arguments and calls the Translator.
     * 
     * Options:
     * -h, --help                prints the usage help message
     * -d, --delimiter <value>   delimiter used in RDF triples if not whitespace
     * -e, --expand              expand prefixes used in the query
     * -opt, --optimize          turn on SPARQL algebra optimization
     * -i, --input <file>        SPARQL query file to translate
     * -o, --output <file>       Pig output script file
     *
     * @param args commandline arguments
     * @throws IOException
     */
    public static void main(String[] args) {
        //parse the commandline arguments
        parseInput(args);
        //instantiate Translator
        Translator translator = new Translator(inputFile, outputFile);
        translator.setOptimizer(optimize);
        translator.setExpandMode(expand);
        translator.setDelimiter(delimiter);
        translator.translateQuery();
    }
    
    
    /**
     * Parses the commandline arguments.
     *
     * @param args commandline arguments
     */
    @SuppressWarnings("static-access")
    private static void parseInput(String[] args){
        // DEFINITION STAGE
        Options options = new Options();
        Option help = new Option("h", "help", false, "print this message");
        options.addOption(help);
        Option optimizer = new Option("opt", "optimize", false, "turn on SPARQL algebra optimization");
        options.addOption(optimizer);
        Option prefixes = new Option("e", "expand", false, "expand URI prefixes");
        options.addOption(prefixes);
        Option delimit = OptionBuilder.withArgName("value")
                                      .hasArg()
                                      .withDescription("delimiter used in RDF triples if not whitespace")
                                      .withLongOpt("delimiter")
                                      .isRequired(false)
                                      .create("d");
        options.addOption(delimit);
        Option input = OptionBuilder.withArgName("file")
                                    .hasArg()
                                    .withDescription("SPARQL query file to translate")
                                    .withLongOpt("input")
                                    .isRequired(true)
                                    .create("i");
        options.addOption(input);
        Option output = OptionBuilder.withArgName("file")
                                     .hasArg()
                                     .withDescription("Pig output script file")
                                     .withLongOpt("output")
                                     .isRequired(false)
                                     .create("o");
        options.addOption(output);
        
        // PARSING STAGE
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            // parse the command line arguments
            cmd = parser.parse(options, args);
        }
        catch(ParseException exp) {
            // error when parsing commandline arguments
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("PigSPARQL", options, true);
            logger.fatal(exp.getMessage(), exp);
            System.exit(-1);
        }
        
        // INTERROGATION STAGE
        if(cmd.hasOption("help")) {
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("PigSPARQL", options, true);
        }
        if(cmd.hasOption("optimize")) {
            optimize = true;
            logger.info("SPARQL Algebra optimization is turned on");
        }
        if(cmd.hasOption("expand")) {
            expand = true;
            logger.info("URI prefix expansion is turned on");
        }
        if(cmd.hasOption("delimiter")) {
            delimiter = cmd.getOptionValue("delimiter");
            logger.info("Delimiter for RDF triples: " + delimiter);
        }
        if(cmd.hasOption("input")) {
            inputFile = cmd.getOptionValue("input");
        }
        if(cmd.hasOption("output")) {
            outputFile = cmd.getOptionValue("output");
        } else {
            outputFile = cmd.getOptionValue("input")+".pig";
        }
    }
      
}
