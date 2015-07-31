package pigsparql.pig.op;

import com.hp.hpl.jena.shared.PrefixMapping;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Alexander Schaetzle
 */
public abstract class PigOpN extends PigOpBase {

    protected List<PigOp> elements = new ArrayList<PigOp>();


    protected PigOpN(PrefixMapping _prefixes) {
        prefixes = _prefixes;
        resultSchema = new ArrayList<String>();
        varsWithNulls = new ArrayList<String>();
    }

    public PigOp get(int index) {
        return elements.get(index);
    }

    public List<PigOp> getElements() {
        return elements;
    }

    public void add(PigOp op) {
        elements.add(op);
    }

    public void add(int index, PigOp op) {
        elements.add(index, op);
    }

    public Iterator<PigOp> iterator() {
        return elements.iterator();
    }

    public int size() {
        return elements.size();
    }

}
