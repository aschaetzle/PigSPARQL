package pigsparql.rdfLoader;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * PIG UDF Loader for extended N-Triples.
 * It supports RDF Data in N-Triples syntax with some extensions.
 * Beyond the syntax of N-Triples it also supports some commonly used Prefixes
 * as well as the Prefixes used in the SP2Bench SPARQL Performance Benchmark. Furthermore it supports
 * Blank Nodes and Typed Literals.
 * The user can decide to expand the prefixes of the Input Triples or not.
 *
 * @see org.apache.pig.LoadFunc
 * @see <a href="http://dbis.informatik.uni-freiburg.de/index.php?project=SP2B">SP2Bench SPARQL Performance Benchmark</a>
 *
 * @author Alexander Schaetzle
 * @version 1.0
 */
public class ExNTriplesLoader extends LoadFunc {

    protected final Log mLog = LogFactory.getLog(getClass());
    protected RecordReader in = null;
    protected String signature;
    
    private String fieldDel;
    private static boolean expand;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    /**
     * Default Constructor of class ExNTriplesLoader.
     * If no arguments are passed to the Loader prefixes will not be expanded by default.
     */
    public ExNTriplesLoader() {
        this(" ", null);
    }
    
    public ExNTriplesLoader(String param) {
        this(param, param);
    }

    /**
     * Constructor of class ExNTriplesLoader.
     * The user can pass the String 'expand' to the constructor to indicate
     * that prefixes should be expanded when loading a Triple.
     *
     * @param _expand   'expand' will cause the Loader to expand prefixes
     */
    public ExNTriplesLoader(String delimiter, String prefixExpand) {
        //fieldDel = StorageUtil.parseFieldDel(delimiter);
        // set the delimiter used to parse the Triples
        if(delimiter != null && !delimiter.equals("expand")) {
            fieldDel = delimiter;
        }
        else {
            fieldDel = " ";
        }
        // check if prefixes should be expanded
        if(prefixExpand != null && prefixExpand.equals("expand")) {
            expand = true;
        }
        else {
            expand = false;
        }
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }
            Text value = (Text) in.getCurrentValue();
            // Tuples are always triples of Subject, Predicate, Object
            Tuple tuple = mTupleFactory.newTuple(3);
            // use the Parser to parse the input line into a Triple
            String[] triple = ExNTriplesParser.parseTriple(value.toString(), fieldDel, expand);
            // if the parser returns null a syntax error occured in this line
            // we then continue with the next line
            if(triple == null) {
                mLog.warn("unable to parse: "+value.toString());
                return getNext();
            }
            // set the fields of the Tuple as Subject, Predicate, Object
            tuple.set(0, new DataByteArray(triple[0])); // Subject
            tuple.set(1, new DataByteArray(triple[1])); // Predicate
            tuple.set(2, new DataByteArray(triple[2])); // Object
            return tuple;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }

    }
    
    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return new ExNTriplesStorageConverter();
    }

    @Override
    public InputFormat getInputFormat() {
        return new PigTextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job)
    throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }
    
    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature;
    }
}
