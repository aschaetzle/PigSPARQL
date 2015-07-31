package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.expr.Expr;
import java.util.ArrayList;
import java.util.Iterator;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;
import pigsparql.sparql.ExprCompiler;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigLeftJoin extends PigOp2 {

    private final OpLeftJoin opLeftJoin;


    public PigLeftJoin(OpLeftJoin _opLeftJoin, PigOp _leftOp, PigOp _rightOp, PrefixMapping _prefixes) {
        super(_leftOp, _rightOp, _prefixes);
        opLeftJoin = _opLeftJoin;
        resultName = Tags.LEFT_JOIN;
    }


    @Override
    public String getName() {
        return Tags.PIG_LEFTJOIN;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        String leftJoin = "-- " + Tags.LEFT_JOIN + "\n";

        if (opLeftJoin.getExprs() == null) {
            OpConditional opCond = new OpConditional(opLeftJoin.getLeft(), opLeftJoin.getRight());
            PigConditional pigCond = new PigConditional(opCond, getLeft(), getRight(), prefixes);
            leftJoin = pigCond.translate(_resultName);
            varsWithNulls = pigCond.getVarsWithNulls();
            resultSchema = pigCond.getSchema();
        }
        else {
            varsWithNulls.addAll(leftOp.getVarsWithNulls());
            varsWithNulls.addAll(rightOp.getVarsWithNulls());
            leftJoin += generateLeftJoin();
        }

        return leftJoin;
    }


    private String generateLeftJoin() {
        String leftJoin = "";
        boolean cross = false;
        ArrayList<String> leftOpSchema = leftOp.getSchema();
        ArrayList<String> rightOpSchema = rightOp.getSchema();
        String joinArg, field;

        ArrayList<String> sharedVars = getSharedVars();
        if (checkForNullJoin(sharedVars)) {
            throw new UnsupportedOperationException("Query can lead to a Join on Null values in OPTIONAL clause!");
        }

        // 1. LEFT OUTER JOIN or CROSS
        String join1 = "";
        if (sharedVars.isEmpty()) {
            join1 += "lj = CROSS " + leftOp.getResultName() + ", " + rightOp.getResultName();
            resultSchema.addAll(leftOpSchema);
            resultSchema.addAll(rightOpSchema);
            cross = true;
        }
        else {
            join1 += "lj = JOIN ";
            joinArg = toArgumentList(sharedVars);
            join1 += leftOp.getResultName() + " BY " + joinArg + " LEFT OUTER, ";
            join1 += rightOp.getResultName() + " BY " + joinArg;
        }
        join1 += " PARALLEL $reducerNum ;\n";
        leftJoin += join1;

        ArrayList<String> rightOuterVars = new ArrayList<String>();
        rightOuterVars.addAll(rightOpSchema);
        rightOuterVars.removeAll(sharedVars);
        varsWithNulls.addAll(rightOuterVars);

        // 2. JOIN Projection -> Foreach
        if (!cross) {
            leftJoin += generateJoinProjection("lj","lj");
        }

        // 3. SPLIT corresponding to Filter expression
        String filter;
        Iterator<Expr> iterator = opLeftJoin.getExprs().iterator();
        Expr current = iterator.next();
        ExprCompiler translator = new ExprCompiler(prefixes);
        filter = translator.translate(current);
        while (iterator.hasNext()) {
            current = iterator.next();
            filter += " AND " + translator.translate(current);
        }
        if(opLeftJoin.getExprs().size() > 1)
            filter = "(" + filter + ")";

        // first field of resultSchema that is not included in schema of leftOp
        String nullField = resultSchema.get(leftOpSchema.size());
        String split1 = "";
        split1 += "SPLIT lj INTO " + resultName + " IF ";
        split1 += cross ? "" : "(" + nullField + " is null) OR ";
        split1 += filter;
        split1 += ", diff IF ";
        split1 += cross ? "" : "(" + nullField + " is not null) AND ";
        split1 += "NOT" + filter;
        split1 += " ;\n";
        leftJoin += split1;

        // 4. Project mappings (that do not satisfy the Filter) on the schema of leftOp
        String foreach1 = "";
        foreach1 += "diff = FOREACH diff GENERATE ";
        field = leftOpSchema.get(0);
        foreach1 += field;
        for (int i=1; i<leftOpSchema.size(); i++) {
            field = leftOpSchema.get(i);
            foreach1 += ", " + field;
        }
        foreach1 += " ;\n";
        leftJoin += foreach1;

        // 5. Distinct -> every mapping in diff only once
        leftJoin += "diff = DISTINCT diff PARALLEL $reducerNum ;\n";

        // 6. LEFT OUTER JOIN of current result and diff
        String join2 = "";
        joinArg = toArgumentList(leftOpSchema);
        join2 += "diff = JOIN diff BY " + joinArg + " LEFT OUTER, ";
        join2 += resultName + " BY " +joinArg;
        join2 += " PARALLEL $reducerNum ;\n";
        leftJoin += join2;

        // 7. JOIN Projection -> Foreach
        String foreach2 = "";
        foreach2 += "diff = FOREACH diff GENERATE ";
        field = leftOpSchema.get(0);
        // schema of left subOp completely
        foreach2 += "diff::" + field + " AS " + field;
        for (int i=1; i<leftOpSchema.size(); i++) {
            field = leftOpSchema.get(i);
            foreach2 += ", " + "diff::" + field + " AS " + field;
        }
        // schema of right subOp without shared variables
        for (int i=0; i<rightOpSchema.size(); i++) {
            field = rightOpSchema.get(i);
            if (!leftOpSchema.contains(field)) {
                foreach2 += ", " + resultName + "::" + field + " AS " + field;
            }
        }
        foreach2 += " ;\n";
        leftJoin += foreach2;

        // 8. FILTER for not matching mappings in diff (use SPLIT because of bug in Pig 0.5.0)
        String split2 = "";
        split2 += "SPLIT diff INTO diff IF " + resultSchema.get(leftOpSchema.size()) + " is null ,";
        split2 += " trash IF " + resultSchema.get(leftOpSchema.size()) + " is not null";
        split2 += " ;\n";
        leftJoin += split2;

        // 9. UNION resultName + diff
        String union = "";
        union += "UNION " + resultName + ", diff";
        union += " ;\n";
        leftJoin += union;

        return leftJoin;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }
    
}
