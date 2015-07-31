package pigsparql.pig;

import java.io.PrintWriter;
import java.util.Iterator;
import pigsparql.pig.op.PigOp;
import pigsparql.pig.op.PigOp1;
import pigsparql.pig.op.PigOp2;
import pigsparql.pig.op.PigOpN;

/**
 * Class for printing a PigOp tree.
 */
public class PigOpPrettyPrinter extends PigOpVisitorByType {

    private PrintWriter writer;
    private String offset;


    /**
     * Private constructor, initialization using factory function.
     * 
     * @param _writer 
     */
    private PigOpPrettyPrinter(PrintWriter _writer) {
        offset = "";
        writer = _writer;
    }

    /**
     * Prints the given PigOp tree.
     * 
     * @param _writer
     * @param op 
     */
    public static void print(PrintWriter _writer, PigOp op) {
        op.visit(new PigOpPrettyPrinter(_writer));
    }

    
    @Override
    protected void visit0(PigOp op) {
        writer.println(offset + op.getName());
    }

    @Override
    protected void visit1(PigOp1 op) {
        writer.println(offset + op.getName() + "(");
        offset += "  ";
        if ( op.getSubOp() != null ) op.getSubOp().visit(this);
        offset = offset.substring(2);
        writer.println(")");
    }

    @Override
    protected void visit2(PigOp2 op) {
        writer.println(offset + op.getName() + "(");
        offset += "  ";
        if ( op.getLeft() != null ) op.getLeft().visit(this);
        if ( op.getRight() != null ) op.getRight().visit(this);
        offset = offset.substring(2);
        writer.println(")");
    }

    @Override
    protected void visitN(PigOpN op) {
        writer.println(offset + op.getName() + "(");
        offset += "  ";
        for ( Iterator<PigOp> iter = op.iterator() ; iter.hasNext() ; )
        {
            PigOp sub = iter.next();
            sub.visit(this);
        }
        offset = offset.substring(2);
        writer.println(")");
    }

}
