package pigsparql.sparql;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.expr.*;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import java.util.Stack;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class ExprTranslator implements ExprVisitor {

    private boolean expandPrefixes;
    private Stack<String> stack;
    private PrefixMapping prefixes;


    public ExprTranslator(PrefixMapping _prefixes) {
        stack = new Stack<String>();
        prefixes = _prefixes;
    }

    public String translate(Expr expr, boolean expandPrefixes) {
        this.expandPrefixes = expandPrefixes;
        ExprWalker.walkBottomUp(this, expr);
        return stack.pop();
    }


    
    @Override
    public void startVisit() { }
    
    public void visit(ExprFunction func) {
        if (func instanceof ExprFunction0) {
            visit((ExprFunction0) func);
        }
        else if (func instanceof ExprFunction1) {
            visit((ExprFunction1) func);
        }
        else if (func instanceof ExprFunction2) {
            visit((ExprFunction2) func);
        }
        else if (func instanceof ExprFunction3) {
            visit((ExprFunction3) func);
        }
        else if (func instanceof ExprFunctionN) {
            visit((ExprFunctionN) func);
        }
        else if (func instanceof ExprFunctionOp) {
            visit((ExprFunctionOp) func);
        }
    }

    @Override
    public void visit(ExprFunction1 func) {
        //System.out.println(func.getOpName());
        boolean before = true;
        String sub = stack.pop();

        String operator = Tags.NO_SUPPORT;
        if (func instanceof E_LogicalNot) {
            if (func.getArg() instanceof E_Bound) {
                operator = Tags.NOT_BOUND;
                sub = sub.substring(1, sub.indexOf(Tags.BOUND));
                before = false;
            }
            else {
                operator = Tags.LOGICAL_NOT;
            }
        }
        else if (func instanceof E_Bound) {
            operator = Tags.BOUND;
            before = false;
        }

        if (operator.equals(Tags.NO_SUPPORT)) {
            throw new UnsupportedOperationException("Filter expression not supported yet!");
        }
        else {
            if (before) {
                stack.push("(" + operator + sub + ")");
            }
            else {
                stack.push("(" + sub + operator + ")");
            }
        }
    }

    @Override
    public void visit(ExprFunction2 func) {
        //System.out.println(func.getOpName());
        String right = stack.pop();
        String left = stack.pop();

        String operator = Tags.NO_SUPPORT;
        if (func instanceof E_GreaterThan) {
            operator = Tags.GREATER_THAN;
        }
        else if (func instanceof E_GreaterThanOrEqual) {
            operator = Tags.GREATER_THAN_OR_EQUAL;
        }
        else if (func instanceof E_LessThan) {
            operator = Tags.LESS_THAN;
        }
        else if (func instanceof E_LessThanOrEqual) {
            operator = Tags.LESS_THAN_OR_EQUAL;
        }
        else if (func instanceof E_Equals) {
            operator = Tags.EQUALS;
        }
        else if (func instanceof E_NotEquals) {
            operator = Tags.NOT_EQUALS;
        }
        else if (func instanceof E_LogicalAnd) {
            operator = Tags.LOGICAL_AND;
        }
        else if (func instanceof E_LogicalOr) {
            operator = Tags.LOGICAL_OR;
        }

        if (operator.equals(Tags.NO_SUPPORT)) {
            throw new UnsupportedOperationException("Filter expression not supported yet!");
        }
        else {
            stack.push("(" + left + operator + right + ")");
        }
    }

    @Override
    public void visit(NodeValue nv) {
        stack.push("'"+FmtUtils.stringForNode(nv.asNode(), prefixes)+"'");
    }

    @Override
    public void visit(ExprVar nv) {
        stack.push(nv.getVarName());
    }

    @Override
    public void visit(ExprFunction0 func) {
        throw new UnsupportedOperationException("ExprFunction0 not supported yet.");
    }

    @Override
    public void visit(ExprFunction3 func) {
        throw new UnsupportedOperationException("ExprFunction3 not supported yet.");
    }
    
    @Override
    public void visit(ExprFunctionN func) {
        throw new UnsupportedOperationException("ExprFunctionN not supported yet!");
    }

    @Override
    public void visit(ExprFunctionOp funcOp) {
        throw new UnsupportedOperationException("ExprFunctionOp not supported yet.");
    }

    @Override
    public void visit(ExprAggregator eAgg) {
        throw new UnsupportedOperationException("ExprAggregator not supported yet.");
    }

    @Override
    public void finishVisit() { }

}
