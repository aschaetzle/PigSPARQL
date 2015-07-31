package pigsparql.pig;

import java.util.Iterator;
import pigsparql.pig.op.PigOp;
import pigsparql.pig.op.PigOp1;
import pigsparql.pig.op.PigOp2;
import pigsparql.pig.op.PigOpN;

/**
 * Applies a given PigOpVisitor to all operators in the tree
 * Can walk through the tree bottom up or top down.
 * 
 * @author Alexander Schaetzle
 * @see pigsparql.pig.PigOpVisitor
 */
public class PigOpWalker extends PigOpVisitorByType {

    // Visitor to be applied to all operators in the tree below the given operator
    private PigOpVisitor visitor;
    // Walk through tree bottom up or top down
    private boolean topDown;


    /**
     * Private constructor, initialization using factory functions.
     * 
     * @param visitor PigOpVisitor to be applied
     * @param topDown true - top down, false - bottom up
     * @see #walkBottomUp(pigsparql.pig.PigOpVisitor, pigsparql.pig.op.PigOp) 
     * @see #walkTopDown(pigsparql.pig.PigOpVisitor, pigsparql.pig.op.PigOp) 
     */
    private PigOpWalker(PigOpVisitor visitor, boolean topDown) {
        this.visitor = visitor;
        this.topDown = topDown;
    }

    /**
     * Apply a given PigOpVisitor to all operators in a PigOp tree walking top down.
     * 
     * @param visitor PigOpVisitor to be applied
     * @param op Root of PigOp tree
     * @see pigsparql.pig.PigOpVisitor
     */
    public static void walkTopDown(PigOpVisitor visitor, PigOp op) {
        op.visit(new PigOpWalker(visitor, true));
    }

    /**
     * Apply a given PigOpVisitor to all operators in a PigOp tree walking bottom up.
     * 
     * @param visitor PigOpVisitor to be applied
     * @param op Root of PigOp tree
     * @see pigsparql.pig.PigOpVisitor
     */
    public static void walkBottomUp(PigOpVisitor visitor, PigOp op) {
        op.visit(new PigOpWalker(visitor, false));
    }

    
    /**
     * Visit leef operator with no sub operators.
     * @param op 
     */
    @Override
    protected void visit0(PigOp op) {
        op.visit(visitor);
    }

    /**
     * Visit operator with 1 sub operator.
     * @param op 
     */
    @Override
    protected void visit1(PigOp1 op) {
        if ( topDown ) op.visit(visitor);
        if ( op.getSubOp() != null ) op.getSubOp().visit(this);
        else logger.warn("Sub operator is missing in " + op.getName());
        if ( !topDown ) op.visit(visitor);
    }

    /**
     * Visit operator with 2 sub operator.
     * @param op 
     */
    @Override
    protected void visit2(PigOp2 op) {
        if ( topDown ) op.visit(visitor);
        if ( op.getLeft() != null ) op.getLeft().visit(this);
        else logger.warn("Left sub operator is missing in " + op.getName());
        if ( op.getRight() != null ) op.getRight().visit(this);
        else logger.warn("Right sub operator is missing in " + op.getName());
        if ( !topDown ) op.visit(visitor);
    }

    /**
     * Visit operator with N sub operator.
     * @param op 
     */
    @Override
    protected void visitN(PigOpN op) {
        if ( topDown ) op.visit(visitor);
        for ( Iterator<PigOp> iter = op.iterator() ; iter.hasNext() ; )
        {
            PigOp sub = iter.next();
            sub.visit(this);
        }
        if ( !topDown ) op.visit(visitor);
    }

}
