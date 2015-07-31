package pigsparql.pig.op;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import java.util.Iterator;
import java.util.List;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigOrder extends PigOp1 {

    private final OpOrder opOrder;


    public PigOrder(OpOrder _opOrder, PigOp _subOp, PrefixMapping _prefixes) {
        super(_subOp, _prefixes);
        opOrder = _opOrder;
        resultName = Tags.ORDER;
    }


    @Override
    public String getName() {
        return Tags.PIG_ORDER;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        varsWithNulls.addAll(subOp.getVarsWithNulls());
        String order = "-- " + Tags.ORDER + "\n";

        order += resultName + " = ORDER " + subOp.getResultName() + " BY ";
        List<SortCondition> conditions = opOrder.getConditions();
        Iterator<SortCondition> iterator = conditions.iterator();
        SortCondition current = iterator.next();
        order += getOrderArg(current);
        while (iterator.hasNext()) {
            current = iterator.next();
            order += ", " + getOrderArg(current);
        }
        order += " PARALLEL $reducerNum ;\n";
        resultSchema.addAll(subOp.getSchema());

        return order;
    }

    private String getOrderArg(SortCondition condition) {
        String orderArg = condition.getExpression().getVarName();
        int direction = condition.getDirection();

        switch(direction) {
            case Query.ORDER_ASCENDING: {
                orderArg += " ASC";
                break;
            }
            case Query.ORDER_DESCENDING: {
                orderArg += " DESC";
                break;
            }
            case Query.ORDER_DEFAULT: {
                break;
            }
        }
        
        return orderArg;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }

}
