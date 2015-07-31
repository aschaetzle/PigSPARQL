package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigSlice extends PigOp1 {

    private final OpSlice opSlice;


    public PigSlice(OpSlice _opSlice, PigOp _subOp, PrefixMapping _prefixes) {
        super(_subOp, _prefixes);
        opSlice = _opSlice;
        resultName = Tags.SLICE;
    }


    @Override
    public String getName() {
        return Tags.PIG_SLICE;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        varsWithNulls.addAll(subOp.getVarsWithNulls());
        String slice = "-- " + Tags.SLICE + "\n";
        int limit = 0;

        if(opSlice.getStart() > 0) {
            System.out.println("Offset not supported yet! First " + opSlice.getStart() + " results must be ignored!");
            limit += opSlice.getStart();
        }
        if(opSlice.getLength() > 0) {
            limit += opSlice.getLength();
            slice += resultName + " = LIMIT " + subOp.getResultName() + " " + limit;
            slice += " ;\n";
        }
        resultSchema.addAll(subOp.getSchema());

        return slice;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }

}
