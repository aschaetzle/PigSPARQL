package pigsparql.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import pigsparql.rdf.SimpleTripleParser;


public class VPDriver extends Configured implements Tool {

	private Path inputPath;
	private Path outputPath;
	private String delimiter = "\t";
	private boolean expand = false;
	private boolean collapse = false;
	private boolean canonicLiteral = false;
	private int reducers = 0;

	// Define a static logger variable so that it references the corresponding Logger instance
	private static final Logger logger = Logger.getLogger(VPDriver.class);


	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new VPDriver(), args);
		System.exit(exitCode);
	}


	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Validate and parse arguments passed from the command line.
		 */
		parseInput(args);

		// Configuration processed by ToolRunner
		Configuration conf = getConf();
		// Notify Hadoop that application uses GenericOptionsParser
		// This is not required but prevents that a warning is printed during execution
		conf.set("mapreduce.client.genericoptionsparser.used", "true");

		// Setting the parameters for Mappers and Reducers parsed from the commandline
		conf.set("rdf.delimiter", delimiter);
		conf.setBoolean("rdf.expand", expand);
		conf.setBoolean("rdf.collapse", collapse);
		conf.setBoolean("rdf.literal", canonicLiteral);

		// Logging
		logger.info("Using delimiter: " + delimiter);
		logger.info("Expanding prefixes: " + expand);
		logger.info("Collapsing prefixes: " + collapse);
		logger.info("Converting literals to canonical representation: " + canonicLiteral);
		logger.info("Using RDF Parser: " + conf.getClass("rdf.parser", SimpleTripleParser.class).getName());

		/*
		 * Create a Job using the processed Configuration conf.
		 * Make sure that all changes to the configuration are set before as Job instantiation
		 * makes a deep copy of the configuration, so changes to conf in later stage
		 * will not be propagated to the Job execution.
		 */
		Job job = Job.getInstance(conf);

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(getClass());

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("VP of " + inputPath.getName() + " into " + outputPath.toString());

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Prevent that empty default output files are generated as we use MultipleOutputs instead
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		// Map-only partitioning or partitioning using Reduce Phase to avoid small files problem of HDFS
		if (reducers == 0) {
			// Set Mapper class
			job.setMapperClass(VPMapOnlyMapper.class);
			// Logging
			logger.info("Vertical Partitioning using Map-Only Job");
		}
		else {
			// Set Mapper class
			job.setMapperClass(VPMapper.class);
			// Set Reducer class
			job.setReducerClass(VPReducer.class);
			// Define Map Output Classes (Key, Value)
			// We have to define this at it is different from the Job Output (Key, Value)
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TextPair.class);
			// Logging
			logger.info("Vertical Partitioning with Reduce Phase to avoid small files");
		}

		// Define Job Output Classes (Key, Value)
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// Set Compression of Output Files
		//FileOutputFormat.setCompressOutput(job, true);
		//FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

		// Set the Number of Reduce Tasks
		// If it is a Map-only partitioning the number of Reduce Tasks is 0
		job.setNumReduceTasks(reducers);
		// Logging
		logger.info("Number of Reducers: " + reducers);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return job.waitForCompletion(true) ? 0 : 1;
	}


	@SuppressWarnings("static-access")
	private void parseInput(String[] args) {
		// DEFINITION STAGE
		Options options = new Options();
		Option expandPrefix = new Option("e", "expand", false, "expand URI prefixes");
		options.addOption(expandPrefix);
		Option collapsePrefix = new Option("c", "collapse", false, "collapse URI prefixes");
		options.addOption(collapsePrefix);
		Option canonicalLiteral = new Option("l", "literal", false, "convert Literals to canonical representation without whitespaces and datatypes");
		options.addOption(canonicalLiteral);
		Option delimit = OptionBuilder.withArgName("delimiter")
				.hasArg()
				.withDescription("delimiter used in RDF triples if not tab")
				.withLongOpt("delimiter")
				.isRequired(false)
				.create("d");
		options.addOption(delimit);
		Option input = OptionBuilder.withArgName("inputPath")
				.hasArg()
				.withDescription("input directory where RDF data is stored in HDFS")
				.withLongOpt("input")
				.isRequired(true)
				.create("i");
		options.addOption(input);
		Option output = OptionBuilder.withArgName("outputPath")
				.hasArg()
				.withDescription("directory where output should be stored in HDFS")
				.withLongOpt("output")
				.isRequired(true)
				.create("o");
		options.addOption(output);
		Option reduceNum = OptionBuilder.withArgName("number of reducers")
				.hasArg()
				.withDescription("number of reducers to be used, set 0 or omit for map-only partitioning")
				.withLongOpt("reduce")
				.withType(int.class)
				.isRequired(false)
				.create("r");
		options.addOption(reduceNum);

		// PARSING STAGE
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			// parse the command line arguments
			// do not accept unknown options
			cmd = parser.parse(options, args, false);
		} catch (ParseException exp) {
			// error when parsing commandline arguments
			System.err.printf("Usage: %s [generic options] [-D rdf.parser=<RDF Parser class>] [-e] [-c] [-l] [-d <value>] -i <inputPath> -o <outputPath> [-r <number of reducers>]\n",
					getClass().getSimpleName());
			System.err.println("-D rdf.parser=<RDF Parser class> \t" + "pigsparql.rdf.ExNTriplesParser or pigsparql.rdf.SimpleTripleParser, default is Simple Parser");
			System.err.println("-e, --expand \t" + expandPrefix.getDescription());
			System.err.println("-c, --collapse \t" + collapsePrefix.getDescription());
			System.err.println("-l, --literal \t" + canonicalLiteral.getDescription());
			System.err.println("-d, --delimiter <value> \t" + delimit.getDescription());
			System.err.println("-i, --input <inputPath> \t" + input.getDescription());
			System.err.println("-o, --output <outputPath> \t" + output.getDescription());
			System.err.println("-r, --reduce <number of reducers> \t" + reduceNum.getDescription());
			System.err.println();
			ToolRunner.printGenericCommandUsage(System.err);
			// Logging
			logger.fatal("An commandline syntax error occurred!", exp);
			System.exit(-1);
		}

		// INTERROGATION STAGE
		// input and output are required
		inputPath = new Path(cmd.getOptionValue("input"));
		outputPath = new Path(cmd.getOptionValue("output"));
		// other options are optional
		if (cmd.hasOption("expand")) {
			expand = true;
		}
		if (cmd.hasOption("collapse")) {
			collapse = true;
		}
		if (cmd.hasOption("literal")) {
			canonicLiteral = true;
		}
		if (cmd.hasOption("delimiter")) {
			delimiter = cmd.getOptionValue("delimiter");
		}
		if (cmd.hasOption("reduce")) {
			reducers = Integer.parseInt(cmd.getOptionValue("reduce"));
		}
	}

}
