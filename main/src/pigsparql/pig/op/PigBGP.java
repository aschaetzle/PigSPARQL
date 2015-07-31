package pigsparql.pig.op;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import pigsparql.pig.PigOpVisitor;
import pigsparql.pig.Tags;

/**
 *
 * @author Alexander Schaetzle
 */
public class PigBGP extends PigOp0 {

    private OpBGP opBGP;
    private ArrayList<String> joinSchema;
    private String joinBlock;


    public PigBGP(OpBGP _opBGP, PrefixMapping _prefixes) {
        super(_prefixes);
        opBGP = _opBGP;
        resultName = Tags.BGP;
    }

    @Override
    public String getName() {
        return Tags.PIG_BGP;
    }


    @Override
    public String translate(String _resultName) {
        resultName = _resultName;
        String bgp = "-- " + Tags.BGP + "\n";

        bgp += filterTriplePatterns();
        bgp += joinTriplePatterns();

        return bgp;
    }


    /**
     * Generate FILTER statements for Triple Patterns.
     * @return 
     */
    private String filterTriplePatterns() {
        List<Triple> triples = opBGP.getPattern().getList();
        Iterator<Triple> triplesIterator = triples.listIterator();
        String triplePatterns = "";

        // empty PrefixMapping when prefixes should be expanded
        if(expandPrefixes) {
            prefixes = PrefixMapping.Factory.create();
        }

        /*
         * Now iterate over all Triples in the BGP:
         * 1. Produce a FILTER for the Triple if there is at least one bound component (s,p,o)
         * 2. Project each Triple on its variables
         */
        int i = 0;
        while(triplesIterator.hasNext()) {
            Triple triple = triplesIterator.next();

            /*
             * FILTER
             */
            String filter = "f"+i+" = FILTER " + Tags.indata + " BY ";
            boolean noFilter = true;
            Node subject = triple.getSubject();
            Node predicate = triple.getPredicate();
            Node object = triple.getObject();
            if(subject.isURI() || subject.isBlank()) {
                // subject is bound -> add to Filter
                filter += "s == '" + FmtUtils.stringForNode(subject, prefixes) + "'";
                noFilter = false;
            }
            if(predicate.isURI()) {
                // predicate is bound -> add to Filter
                filter += (noFilter ? "" : " AND ") + "p == '" + FmtUtils.stringForNode(predicate, prefixes) + "'";
                noFilter = false;
            }
            if(object.isURI() || object.isLiteral() || object.isBlank()) {
                // object is bound -> add to Filter
                filter += (noFilter ? "" : " AND ") + "o == '" + FmtUtils.stringForNode(object, prefixes) + "'";
                noFilter = false;
            }
            filter += " ;";
            // output Filter only if there is at least one argument to the Filter
            if(!noFilter) {
                triplePatterns += filter + "\n";
            }

            /*
             * PROJECTION on the variables
             */
            // there will be no joins if the BGP is of size 1 -> Triple gets named with the resultName
            String projection = (triples.size() == 1) ? resultName : "t"+i;
            // Projection on the filtered Triple or indata if there is no Filter for this Triple (nothing bound)
            projection += " = FOREACH " + (noFilter ? Tags.indata : "f"+i) + " GENERATE ";
            ArrayList<String> tripleSchema = getSchemaOfTriple(triple);
            boolean noProject = true;
            if(!tripleSchema.get(0).equals(Tags.NO_VAR)) {
                // subject is a variable
                projection += "s AS " + tripleSchema.get(0);
                noProject = false;
            }
            if(!tripleSchema.get(1).equals(Tags.NO_VAR)) {
                // predicate is a variable
                projection += (noProject ? "" : ", ") + "p AS " + tripleSchema.get(1);
                noProject = false;
            }
            if(!tripleSchema.get(2).equals(Tags.NO_VAR)) {
                // object is a variable
                projection += (noProject ? "" : ", ") + "o AS " + tripleSchema.get(2);
                noProject = false;
            }
            projection += " ;";
            // output Projection only if there is at least one variable in the Triple
            if(!noProject) {
                triplePatterns += projection + "\n";
            }

            i++;
        }

        return triplePatterns;
    }


    /**
     * Generate JOIN statements for Triple Patterns.
     * @return 
     */
    private String joinTriplePatterns() {
        List<Triple> triples = opBGP.getPattern().getList();
        joinBlock = "";

        Triple triple = triples.get(0);
        ArrayList<String> tripleVars = getVarsOfTriple(triple);
        // Schema of the first Triple is completely added to the resultSchema -> no duplicates possible
        resultSchema.addAll(tripleVars);

        int nextTriple = 1;
        ArrayList<String> joinVars;

        /*
         * Iterate over all Triples in the BGP:
         * 1. If a Triple has no shared variables with the current resultSchema we have to do a CROSS
         * 2. If a Triple ha shared variables with the current resultSchema we can do a JOIN
         * Successive CROSS statements can be reduced to one big CROSS
         * Successive JOIN statements on the same variables can be reduced to one big JOIN
         */
        while (nextTriple < triples.size()) {
            triple = triples.get(nextTriple);
            tripleVars = getVarsOfTriple(triple);
            joinVars = getSharedVars(resultSchema, tripleVars);
            int endPos;
            if (joinVars.isEmpty()) {
                /*
                 * Triple has no shared variables with the current resultSchema -> CROSS
                 * nextCross will generate one CROSS of all successive Triples with no shared variables
                 * nextCross returns the index of the last Triple in the generated CROSS
                 */
                endPos = nextCross(nextTriple);
            }
            else {
                /*
                 * Triple has shared variables with the current resultSchema -> JOIN
                 * nextJoin will generate one JOIN of all successive Triples with the same Join variables
                 * nextJoin returns the index of the last Triple in the generated JOIN
                 */
                endPos = nextJoin(nextTriple, joinVars);
            }
            // proceed with next Triple
            nextTriple = endPos + 1;
        }

        return joinBlock;
    }


    /**
     * Cross Product between Triple Patterns.
     * Hopefully we never have to use this.
     * @param start
     * @return 
     */
    private int nextCross(int start) {
        ArrayList<String> newResultSchema = new ArrayList<String>();
        ArrayList<String> combinedSchema = new ArrayList<String>();
        combinedSchema.addAll(resultSchema);

        List<Triple> triples = opBGP.getPattern().getList();
        Triple triple = triples.get(start);
        ArrayList<String> tripleVars = getVarsOfTriple(triple);

        String cross = resultName + " = CROSS ";
        String crossArgs = "";

        /*
         * OPTIMIZATION:
         * Largest relation should be the last entry of the CROSS statement
         * 1. If there is no proceeding JOIN or CROSS statements (start = 1) t0 should be the first entry of the CROSS (highest selectivity)
         * 2. If there are proceeding JOIN or CROSS statements (start > 1) the result of these statements should be the last entry of the CROSS
         */
        if (start == 1) {
            // CROSS starts with t0, t(start)=t1
            crossArgs = "t0, t1";
            // add Schema of t0 (= resultSchema) to newResultSchema
            newResultSchema.addAll(resultSchema);
            combinedSchema.addAll(resultSchema);
        }
        else {
            // CROSS starts with t(start)
            crossArgs += "t" + start;
        }
        // add Schema of t(start) to newResultSchema
        newResultSchema.addAll(tripleVars);
        combinedSchema.addAll(tripleVars);

        /*
         * Successive Triples which also result in a CROSS can be added to this CROSS
         */
        ArrayList<String> sharedVars;
        int endIndex = start+1;
        while (endIndex < triples.size()) {
            triple = triples.get(endIndex);
            tripleVars = getVarsOfTriple(triple);
            sharedVars = getSharedVars(combinedSchema, tripleVars);
            if (sharedVars.isEmpty()) {
                // add Triple to this CROSS
                crossArgs += ", t" + endIndex;
                // add Schema of the Triple to newResultSchema
                newResultSchema.addAll(tripleVars);
                combinedSchema.addAll(tripleVars);
                endIndex++;
            }
            // Triple has shared variables -> END OF CROSS
            else break;
        }

        // if this is not the first CROSS/JOIN (start > 1) -> add proceeding results to the end of this CROSS
        if (start > 1) {
            crossArgs += ", " + resultName;
            newResultSchema.addAll(resultSchema);
        }

        cross += crossArgs + " PARALLEL $reducerNum ;";
        joinBlock += cross + "\n";
        // set the valid resultSchema after this CROSS
        resultSchema = newResultSchema;
        
        return endIndex - 1;
    }


    /**
     * 
     * @param start
     * @param joinVars
     * @return 
     */
    private int nextJoin(int start, ArrayList<String> joinVars) {
        // joinSchema contains the schema of the Triples added by this JOIN
        joinSchema = new ArrayList<String>();
        ArrayList<String> combinedSchema = new ArrayList<String>();
        combinedSchema.addAll(resultSchema);

        // Triples are joined on the joinVars
        String joinArg = toArgumentList(joinVars);
        List<Triple> triples = opBGP.getPattern().getList();
        Triple triple = triples.get(start);
        ArrayList<String> tripleVars = getVarsOfTriple(triple);

        /*
         * JOIN
         */
        String join = resultName + " = JOIN ";
        String joinTriples = "t" + start + " BY " + joinArg;
        joinSchema.addAll(tripleVars);
        combinedSchema.addAll(tripleVars);

        /*
         * Successive Triples which also result in a JOIN on the same variables can be added to this JOIN
         */
        ArrayList<String> sharedVars;
        int endIndex = start+1;
        while (endIndex < triples.size()) {
            triple = triples.get(endIndex);
            tripleVars = getVarsOfTriple(triple);
            sharedVars = getSharedVars(combinedSchema, tripleVars);
            if (joinVars.containsAll(sharedVars) && sharedVars.containsAll(joinVars)) {
                // joinVars and sharedVars contain the same elements (perhaps not in the same order) -> add Triple to this JOIN
                joinTriples += ", t" + endIndex + " BY " + joinArg;
                // add Schema of the Triple to joinSchema
                joinSchema.addAll(tripleVars);
                combinedSchema.addAll(tripleVars);
                endIndex++;
            }
            // Triple can't be added to this JOIN -> END OF JOIN
            else break;
        }

        /*
         * OPTIMIZATION:
         * Largest relation should be the last entry of the JOIN statement
         * 1. If there is no proceeding JOIN or CROSS statements (start = 1) t0 should be the first entry of the JOIN (highest selectivity)
         * 2. If there are proceeding JOIN or CROSS statements (start > 1) the result of these statements should be the last entry of the JOIN
         */
        if (start == 1) {
            joinTriples = "t0 BY " + joinArg + ", " + joinTriples;
        }
        else {
            joinTriples += ", " + resultName + " BY " + joinArg;
        }

        join += joinTriples + " PARALLEL $reducerNum ;";
        joinBlock += join + "\n";


        /*
         * PROJECTION -> drop duplicated entries generated by the JOIN statement
         */
        String foreach = resultName + " = FOREACH " + resultName + " GENERATE ";
        ArrayList<String> newResultSchema = new ArrayList<String>();
        String field;
        boolean first = true;
        int pos = 0;
        // if there are no proceeding JOIN or CROSS statements (start = 1) the JOIN starts with t0 (resultSchema before JOIN contains only schema of t0)
        if (start == 1) {
            for (int i=0; i<resultSchema.size(); i++) {
                field = resultSchema.get(i);
                foreach += (first ? "$" : ", $") + pos + " AS " + field;
                newResultSchema.add(field);
                first = false;
                pos++;
            }
        }
        // add a field of the joinedSchema only to newResultSchema if it is not already contained in the old resultSchema
        for (int i=0; i<joinSchema.size(); i++) {
            field = joinSchema.get(i);
            if (!resultSchema.contains(field)) {
                foreach += (first ? "$" : ", $") + pos + " AS " + field;
                newResultSchema.add(field);
                first = false;
            }
            pos++;
        }
        // if there are proceeding JOIN or CROSS statements (start > 1) -> results added as last entry of JOIN
        if (start > 1) {
            for (int i=0; i<resultSchema.size(); i++) {
                field = resultSchema.get(i);
                foreach += (first ? "$" : ", $") + pos + " AS " + field;
                newResultSchema.add(field);
                first = false;
                pos++;
            }
        }

        foreach += " ;";
        joinBlock += foreach + "\n";
        // set the valid resultSchema after this JOIN
        resultSchema = newResultSchema;
        
        return endIndex - 1;
    }


    private ArrayList<String> getSchemaOfTriple(Triple t) {
        ArrayList<String> schema = new ArrayList<String>();
        Node subject = t.getSubject();
        Node predicate = t.getPredicate();
        Node object = t.getObject();
        if(subject.isVariable())
            schema.add(subject.getName());
        else
            schema.add(Tags.NO_VAR);
        if(predicate.isVariable())
            schema.add(predicate.getName());
        else
            schema.add(Tags.NO_VAR);
        if(object.isVariable())
            schema.add(object.getName());
        else
            schema.add(Tags.NO_VAR);
        return schema;
    }


    private ArrayList<String> getVarsOfTriple(Triple t) {
        ArrayList<String> vars = new ArrayList<String>();
        Node subject = t.getSubject();
        Node predicate = t.getPredicate();
        Node object = t.getObject();
        if(subject.isVariable())
            vars.add(subject.getName());
        if(predicate.isVariable())
            vars.add(predicate.getName());
        if(object.isVariable())
            vars.add(object.getName());
        return vars;
    }


    private ArrayList<String> getSharedVars(ArrayList<String> leftSchema, ArrayList<String> rightSchema) {
        ArrayList<String> sharedVars = new ArrayList<String>();
        for(int i=0; i<rightSchema.size(); i++) {
            if(leftSchema.contains(rightSchema.get(i)))
                sharedVars.add(rightSchema.get(i));
        }
        return sharedVars;
    }


    @Override
    public void visit(PigOpVisitor pigOpVisitor) {
        pigOpVisitor.visit(this);
    }


    /* OLD CODE:
     * Aufeinanderfolgende Joins oder Cross werden nicht zusammengefuehrt.
     * In jedem Join/Cross kommt ein weiteres Tripel hinzu.
     *
     *
    private String generateJoinBlock() {
        List<Triple> triples = opBGP.getPattern().getList();
        Iterator<Triple> triplesIterator = triples.listIterator();
        String joinBlock = "";

        joinSchema = new ArrayList<String>();
        Triple triple = triplesIterator.next();
        ArrayList<String> tripleVars = getVarsOfTriple(triple);
        joinSchema.addAll(tripleVars);

        int i = 1;
        while(triplesIterator.hasNext()) {
            triple = triplesIterator.next();
            tripleVars = getVarsOfTriple(triple);
            ArrayList<String> sharedVars = getSharedVars(joinSchema, tripleVars);

            if(sharedVars.isEmpty()) {
                joinBlock += generateCross(i, tripleVars);
                joinSchema.addAll(tripleVars);
            }
            else {
                joinBlock += generateJoin(i, sharedVars, tripleVars);
                tripleVars.removeAll(sharedVars);
                joinSchema.addAll(tripleVars);
            }
            i++;
        }

        resultSchema = new ArrayList<String>();
        resultSchema.addAll(joinSchema);

        return joinBlock;
    }


    private String generateJoin(int i, ArrayList<String> sharedVars, ArrayList<String> tripleVars) {
        String joinArg = toArgumentList(sharedVars);
        String join = resultName + " = JOIN ";
        if(i == 1) {
            join += "t0 BY " + joinArg;
            join += ", t1 BY " + joinArg;
        }
        else {
            join += "t"+ i + " BY " + joinArg;
            join += ", " + resultName + " BY " + joinArg;
        }
        join += " PARALLEL $reducerNum ;\n";

        join += resultName + " = FOREACH " + resultName + " GENERATE ";
        join += "$0 AS " + joinSchema.get(0);
        for(int j=1; j<joinSchema.size(); j++) {
            join += ", $" + j + " AS " + joinSchema.get(j);
        }
        for(int j=0; j<tripleVars.size(); j++) {
            if(!sharedVars.contains(tripleVars.get(j)))
                join += ", $" + (joinSchema.size()+j) + " AS " + tripleVars.get(j);
        }
        join += " ;\n";

        return join;
    }


    private String generateCross(int i, ArrayList<String> tripleVars) {
        String cross = resultName + " = CROSS ";
        if(i == 1) {
            cross += "t0, t1";
        }
        else {
            cross += "t"+ i + ", " + resultName;
        }
        cross += " PARALLEL $reducerNum ;\n";

        return cross;
    }
     */

}