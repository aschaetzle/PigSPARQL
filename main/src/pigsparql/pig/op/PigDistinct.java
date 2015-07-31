package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigDistinct extends PigOp1 {

    private OpDistinct opDistinct;


    public PigDistinct(OpDistinct _opDistinct, PigOp _subOp, PrefixMapping _prefixes) {
        super(_subOp, _prefixes);
        opDistinct = _opDistinct;
        resultName = Tags.DISTINCT;
    }


    @Override
    public String getName() {
        return Tags.PIG_DISTINCT;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        varsWithNulls.addAll(subOp.getVarsWithNulls());
        String distinct = "-- " + Tags.DISTINCT + "\n";

        distinct += resultName + " = DISTINCT " + subOp.getResultName() + " PARALLEL $reducerNum ;\n";
        resultSchema.addAll(subOp.getSchema());

        return distinct;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }

}
