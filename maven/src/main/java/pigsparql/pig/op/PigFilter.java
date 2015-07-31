package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.expr.Expr;
import java.util.Iterator;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;
import pigsparql.sparql.ExprCompiler;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigFilter extends PigOp1 {

    private final OpFilter opFilter;


    public PigFilter(OpFilter _opFilter, PigOp _subOp, PrefixMapping _prefixes) {
        super(_subOp, _prefixes);
        opFilter = _opFilter;
        resultName = Tags.FILTER;
    }


    @Override
    public String getName() {
        return Tags.PIG_FILTER;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        varsWithNulls.addAll(subOp.getVarsWithNulls());
        String filter = "-- " + Tags.FILTER + "\n";

        filter += resultName + " = FILTER " + subOp.getResultName() + " BY ";
        
        Iterator<Expr> iterator = opFilter.getExprs().iterator();
        Expr current = iterator.next();
        ExprCompiler translator = new ExprCompiler(prefixes);
        filter += translator.translate(current);
        while (iterator.hasNext()) {
            current = iterator.next();
            filter += " AND " + translator.translate(current);
        }
        if(opFilter.getExprs().size() > 1)
            filter = "(" + filter + ")";
        filter += " ;\n";

        resultSchema.addAll(subOp.getSchema());

        return filter;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }

}
