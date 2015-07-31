package pigsparql.sparql;

import pigsparql.pig.op.PigSlice;
import pigsparql.pig.op.PigFilter;
import pigsparql.pig.op.PigReduced;
import pigsparql.pig.op.PigDistinct;
import pigsparql.pig.op.PigJoin;
import pigsparql.pig.op.PigProject;
import pigsparql.pig.op.PigOp;
import pigsparql.pig.op.PigUnion;
import pigsparql.pig.op.PigSequence;
import pigsparql.pig.op.PigBGP;
import pigsparql.pig.op.PigOrder;
import pigsparql.pig.op.PigConditional;
import pigsparql.pig.op.PigLeftJoin;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.*;
import java.util.Stack;

/**
 *
 * @author Alexander Schaetzle
 */
public class AlgebraTransformer extends OpVisitorBase {

    private Stack<PigOp> stack;
    private PrefixMapping prefixes;


    public AlgebraTransformer(PrefixMapping _prefixes) {
        stack = new Stack<PigOp>();
        prefixes = _prefixes;
    }

    public PigOp transform(Op op) {
        AlgebraWalker.walkBottomUp(this, op);
        return stack.pop();
    }


    
    @Override
    public void visit(OpBGP opBGP) {
        stack.push(new PigBGP(opBGP, prefixes));
    }

    @Override
    public void visit(OpFilter opFilter) {
        PigOp subOp = stack.pop();
        stack.push(new PigFilter(opFilter, subOp, prefixes));
    }

    @Override
    public void visit(OpJoin opJoin) {
        PigOp rightOp = stack.pop();
        PigOp leftOp = stack.pop();
        stack.push(new PigJoin(opJoin, leftOp, rightOp, prefixes));
    }

    @Override
    public void visit(OpSequence opSequence) {
        PigSequence pigSequence = new PigSequence(opSequence, prefixes);
        for(int i=0; i<opSequence.size(); i++) {
            pigSequence.add(0,stack.pop());
        }
        stack.push(pigSequence);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        PigOp rightOp = stack.pop();
        PigOp leftOp = stack.pop();
        stack.push(new PigLeftJoin(opLeftJoin, leftOp, rightOp, prefixes));
    }

    @Override
    public void visit(OpConditional opConditional) {
        PigOp rightOp = stack.pop();
        PigOp leftOp = stack.pop();
        stack.push(new PigConditional(opConditional, leftOp, rightOp, prefixes));
    }

    @Override
     public void visit(OpUnion opUnion) {
        PigOp rightOp = stack.pop();
        PigOp leftOp = stack.pop();
        stack.push(new PigUnion(opUnion, leftOp, rightOp, prefixes));
    }

    @Override
    public void visit(OpProject opProject) {
        PigOp subOp = stack.pop();
        stack.push(new PigProject(opProject, subOp, prefixes));
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        PigOp subOp = stack.pop();
        stack.push(new PigDistinct(opDistinct, subOp, prefixes));
    }

    @Override
    public void visit(OpOrder opOrder) {
        PigOp subOp = stack.pop();
        stack.push(new PigOrder(opOrder, subOp, prefixes));
    }

    @Override
    public void visit(OpSlice opSlice) {
        PigOp subOp = stack.pop();
        stack.push(new PigSlice(opSlice, subOp, prefixes));
    }

    @Override
    public void visit(OpReduced opReduced) {
        PigOp subOp = stack.pop();
        stack.push(new PigReduced(opReduced, subOp, prefixes));
    }

}
