package pigsparql.pig;

import pigsparql.rdf.PrefixMode;

/**
 *
 * @author Alexander Schaetzle
 */
public final class Tags {
	
	// Suppress default constructor for noninstantiability
    private Tags() {}

    // Global Constants
    public static final String PIG_BGP                  = "PigBGP";
    public static final String PIG_FILTER               = "PigFilter";
    public static final String PIG_JOIN                 = "PigJoin";
    public static final String PIG_SEQUENCE             = "PigSequenceJoin";
    public static final String PIG_LEFTJOIN             = "PigLeftJoin";
    public static final String PIG_CONDITIONAL          = "PigConditional";
    public static final String PIG_UNION                = "PigUnion";
    public static final String PIG_PROJECT              = "PigProject";
    public static final String PIG_DISTINCT             = "PigDistinct";
    public static final String PIG_ORDER                = "PigOrder";
    public static final String PIG_SLICE                = "PigSlice";
    public static final String PIG_REDUCED              = "PigReduced";

    public static final String BGP                      = "BGP";
    public static final String FILTER                   = "FILTER";
    public static final String JOIN                     = "JOIN";
    public static final String SEQUENCE                 = "SEQUENCE_JOIN";
    public static final String LEFT_JOIN                = "OPTIONAL";
    public static final String CONDITIONAL              = "OPTIONAL";
    public static final String UNION                    = "UNION";
    public static final String PROJECT                  = "SM_Project";
    public static final String DISTINCT                 = "SM_Distinct";
    public static final String ORDER                    = "SM_Order";
    public static final String SLICE                    = "SM_Slice";
    public static final String REDUCED                  = "SM_Reduced";

    public static final String GREATER_THAN             = " > ";
    public static final String GREATER_THAN_OR_EQUAL    = " >= ";
    public static final String LESS_THAN                = " < ";
    public static final String LESS_THAN_OR_EQUAL       = " <= ";
    public static final String EQUALS                   = " == ";
    public static final String NOT_EQUALS               = " != ";
    public static final String LOGICAL_AND              = " AND ";
    public static final String LOGICAL_OR               = " OR ";
    public static final String LOGICAL_NOT              = "NOT ";
    public static final String BOUND                    = " is not null";
    public static final String NOT_BOUND                = " is null";

    public static final String NO_VAR                   = "#noVar";
    public static final String NO_SUPPORT               = "#noSupport";

    // Global Fields
    public static String delimiter                      = " ";
    public static String udf                            = "pigsparql-2.0.jar";
    public static String indata                         = "inputData";
    public static String outdata                        = "outputData";
    public static String rdfLoader                      = "pigsparql.udf.NTriplesStorage";
    public static String vpLoader                       = "PigStorage('\\t')";
    public static String resultWriter                   = "PigStorage";

    public static PrefixMode pMode						= PrefixMode.NO_OP;
    public static boolean verticalPartitioning          = false;    
    public static Optimizer optimizer                   = Optimizer.BGP;
    
    // Enums
    // Levels for SPARQL algebra optimization
    public enum Optimizer {
    	NONE, BGP, FILTER, ALL
    }
    
}
