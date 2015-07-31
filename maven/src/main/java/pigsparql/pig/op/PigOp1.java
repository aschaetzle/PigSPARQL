package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import java.util.ArrayList;

/**
 *
 * @author Alexander Schaetzle
 */
public abstract class PigOp1 extends PigOpBase {

    protected PigOp subOp;


    protected PigOp1(PigOp _subOp, PrefixMapping _prefixes) {
        subOp = _subOp;
        prefixes = _prefixes;
        resultSchema = new ArrayList<String>();
        varsWithNulls = new ArrayList<String>();
    }

    public PigOp getSubOp() {
        return subOp;
    }

}
