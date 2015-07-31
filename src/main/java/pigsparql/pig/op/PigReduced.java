package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;

import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigReduced extends PigOp1 {

    @SuppressWarnings("unused")
	private final OpReduced opReduced;


    public PigReduced(OpReduced _opReduced, PigOp _subOp, PrefixMapping _prefixes) {
        super(_subOp, _prefixes);
        opReduced = _opReduced;
        resultName = Tags.REDUCED;
    }


    @Override
    public String getName() {
        return Tags.PIG_REDUCED;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        varsWithNulls.addAll(subOp.getVarsWithNulls());
        String reduced = "-- " + Tags.REDUCED + "\n";

        reduced += resultName + " = DISTINCT " + subOp.getResultName() + " PARALLEL $reducerNum ;\n";
        resultSchema.addAll(subOp.getSchema());

        return reduced;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }

}
