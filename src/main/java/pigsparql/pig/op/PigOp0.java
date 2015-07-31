package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import java.util.ArrayList;

/**
 *
 * @author Alexander Schaetzle
 */
public abstract class PigOp0 extends PigOpBase {

    protected PigOp0(PrefixMapping _prefixes) {
        prefixes = _prefixes;
        resultSchema = new ArrayList<String>();
        varsWithNulls = new ArrayList<String>();
    }

}
