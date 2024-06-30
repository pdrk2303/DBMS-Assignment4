package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.math.BigDecimal;
import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                         List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
//        System.out.println("PProject relNode is open: " + getInput());
        RelNode child = getInput();
        PRel pRel = (PRel) child;
        return pRel.open();
//        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        return;
    }

    private Object[] current_row;
    private int has_current_row = 0;
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        RelNode child = getInput();
        PRel pRel = (PRel) child;
//        System.out.println("PProject hasnext: " + pRel);
        boolean flag =  pRel.hasNext();
        if (flag) {
            Object[] row = pRel.next();
            while(row == null) {
                if (pRel.hasNext()) {
                    row = pRel.next();
                } else {
                    return false;
                }
            }
            has_current_row = 1;
            current_row = row;
            return true;
        }
        return false;
//        return true;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        /* Write your code here */
//        System.out.println("PProject Next");
//        RelNode child = getInput();
//        PRel pRel = (PRel) child;
//        if (!pRel.hasNext()) {
//            return null;
//        }
//        Object[] row = pRel.next();

        Object[] row;
        if (has_current_row == 1) {
            has_current_row = 0;
            row = current_row;
        } else {
            RelNode child = getInput();
            PRel pRel = (PRel) child;
            row = pRel.next();
        }

        if (row==null) {
            return null;
        }

//        for (Object value: row){
//            System.out.print(value);
//            System.out.print(" ");
//        }
//        System.out.println();

        // Object[] row = pRel.next();
        List<? extends RexNode> projects = getProjects();
        Object[] result = new Object[projects.size()];

        for (int i=0; i<projects.size(); i++) {
            RexNode r = projects.get(i);
            RexInputRef inputRef = (RexInputRef) r;
//            System.out.println("hello");
            int index = inputRef.getIndex();
//            System.out.println("index: " + index);
            if (index >= 0 && index < row.length) {
                result[i] = row[index];
            } else {
                return null;
            }
        }
//        for (Object val: resRow) {
//            System.out.print(val+" ");
//        }
//        System.out.println("\nReturning");
        return result;

        //return row;
    }
}
