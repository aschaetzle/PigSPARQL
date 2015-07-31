package pigsparql.run;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import pigsparql.pig.Tags.Optimizer;
import pigsparql.pig.PigCompiler;
import pigsparql.rdf.PrefixMode;


/**
 * Main Class for program start.
 * Parses the commandline arguments and calls the PigSPARQL translator.
 * 
 * Options:
 * -h, --help prints the usage help message
 * -d, --delimiter <value> delimiter used in RDF triples if not whitespace
 * -e, --expand expand prefixes used in the query
 * -opt, --optimize turn on SPARQL algebra optimization
 * -i, --input <file> SPARQL query file to translate
 * -o, --output <file> Pig output script file
 * 
 * @author Alexander Schaetzle
 */
public class QueryCompiler {

	// Default values if not overwritten by commandline arguments
	private static String inputFile;
	private static String outputFile;
	private static String delimiter = " ";
	private static Optimizer opt = Optimizer.BGP;
	private static PrefixMode pMode = PrefixMode.NO_OP;
	private static boolean verticalPartitioning = false;

	// Define a static logger variable so that it references the corresponding Logger instance
	private static final Logger logger = Logger.getLogger(QueryCompiler.class);


	/**
	 * Main method invoked on program start.
	 * It parses the commandline arguments and calls the PigCompiler.
	 * 
	 * Options:
	 * -h, --help prints the usage help message
	 * -d, --delimiter <value> delimiter used in RDF triples if not whitespace
	 * -e, --expand expand prefixes used in the query
	 * -opt, --optimize turn on SPARQL algebra optimization
	 * -i, --input <file> SPARQL query file to translate
	 * -o, --output <file> Pig output script file
	 * 
	 * @param args
	 *            commandline arguments
	 */
	public static void main(String[] args) {
		// parse the commandline arguments
		parseInput(args);
		// instantiate PigCompiler
		PigCompiler compiler = new PigCompiler(inputFile, outputFile);
		// Optimization level
		logger.info("SPARQL algebra optimization level set to: " + opt.toString());
		compiler.setOptimizer(opt);
		// Vertical Partitioning
		logger.info("Query compilation uses Vertical Partitioning: " + verticalPartitioning);
		compiler.setPartitioning(verticalPartitioning);
		// URI expansion
		logger.info("URI prefix mode is set to: " + pMode.toString());
		compiler.setPrefixMode(pMode);
		// Delimiter
		logger.info("Delimiter for RDF triples: " + delimiter);
		compiler.setDelimiter(delimiter);
		// Query compilation
		compiler.translateQuery();
		logger.info("Query compilation finished!");
	}


	/**
	 * Parses the commandline arguments.
	 * 
	 * @param args
	 *            commandline arguments
	 */
	@SuppressWarnings("static-access")
	private static void parseInput(String[] args) {
		// DEFINITION STAGE
		Options options = new Options();
		Option help = new Option("h", "help", false, "print this message");
		options.addOption(help);
		Option prefixes = new Option("e", "expand", false, "expand URI prefixes");
		options.addOption(prefixes);
		Option collapsePrefix = new Option("c", "collapse", false, "collapse URI prefixes");
		options.addOption(collapsePrefix);
		Option partitioning = new Option("vp", "partitioned", false, "use Vertical Partitioning");
		options.addOption(partitioning);
		Option delimit = OptionBuilder.withArgName("value")
				.hasArg()
				.withDescription("delimiter used in RDF triples (default: whitespace)")
				.withLongOpt("delimiter")
				.isRequired(false)
				.create("d");
		options.addOption(delimit);
		Option optimizer = OptionBuilder.withArgName("value")
				.hasArg()
				.withDescription("SPARQL algebra optimization level: NONE, BGP, FILTER or ALL (default is BGP)")
				.withLongOpt("optimize")
				.isRequired(false)
				.create("opt");
		options.addOption(optimizer);
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
		} catch (ParseException exp) {
			// error when parsing commandline arguments
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("PigSPARQL", options, true);
			logger.fatal(exp.getMessage(), exp);
			System.exit(-1);
		}

		// INTERROGATION STAGE
		if (cmd.hasOption("help")) {
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("PigSPARQL", options, true);
			System.exit(0);
		}
		if (cmd.hasOption("expand")) {
			pMode = PrefixMode.EXPAND;
		}
		if (cmd.hasOption("collapse")) {
			pMode = PrefixMode.COLLAPSE;
		}
		if (cmd.hasOption("delimiter")) {
			delimiter = cmd.getOptionValue("delimiter");
		}
		if (cmd.hasOption("partitioned")) {
			verticalPartitioning = true;
			delimiter = "\\t";
		}
		if (cmd.hasOption("optimize")) {
			switch (cmd.getOptionValue("optimize").toUpperCase()) {
			case "NONE":
				opt = Optimizer.NONE;
				break;
			case "BGP":
				opt = Optimizer.BGP;
				break;
			case "FILTER":
				opt = Optimizer.FILTER;
				break;
			case "ALL":
				opt = Optimizer.ALL;
				break;
			default:
				logger.error("Not a valid optimization level, using default (BGP) instead!");
				opt = Optimizer.BGP;
			}
		}
		if (cmd.hasOption("input")) {
			inputFile = cmd.getOptionValue("input");
		}
		if (cmd.hasOption("output")) {
			outputFile = cmd.getOptionValue("output");
			if (!outputFile.endsWith(".pig"))
				outputFile = outputFile + ".pig";
		} else {
			int filetypePos = inputFile.lastIndexOf(".");
			if (filetypePos != -1)
				outputFile = inputFile.substring(0, filetypePos) + ".pig";
			else
				outputFile = inputFile + ".pig";
		}
	}

}
