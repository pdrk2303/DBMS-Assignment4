package rel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

public class PSort extends Sort implements PRel{

    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
    ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }
    @Override
    public String toString() {
        return "PSort";
    }

    // returns true if successfully opened, false otherwise
    private List<Object[]> accumulatedRows = new ArrayList<>();
    private int index = 0;
    private int limit_count = 0;
    private int size = 0;
    private int rows_to_skip = 0;
    private int limit = 0;

    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
//        System.out.println("Opening PSort");
//        System.out.println("Cluster: " + getCluster());
//        System.out.println("TraitSet: " + getTraitSet());
//        System.out.println("Hints: " + getHints());
//        System.out.println("Child: " + getInput());
//        System.out.println("Collation: " + getCollation());
//        System.out.println("Offset: " + offset);
//        System.out.println("Fetch: " + fetch);
//        System.out.println("Row Type: " + getRowType());

        RelNode child = getInput();
        PRel pRel = (PRel) child;
        boolean isOpen = pRel.open();

        if (!isOpen) {
            return false;
        }
        boolean f = pRel.hasNext();
        // System.out.println("Flag: " + pRel);
        while(f) {
            Object[] row = pRel.next();
            if (row != null) {
//                System.out.println("Row is not NULL");
                accumulatedRows.add(row);
//                System.out.println("Row is not NULL");
            }
//            if (pRel == null) {
//                System.out.println("iiiiiiiiiii");
//            }
            f = pRel.hasNext();
        }
//        System.out.println("Got all rows");
        size = accumulatedRows.size();

//        System.out.println("Collation: " + collation);
//        List<Integer> column_inds = new ArrayList();
//        List<Boolean> order = new ArrayList();
//        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
//            int columnIndex = fieldCollation.getFieldIndex(); // Get the column index
//            boolean isDescending = fieldCollation.getDirection().isDescending(); // Check if the sort direction is descending
//            String sortDirection = isDescending ? "DESC" : "ASC"; // Convert to string representation
//
//            System.out.println("Column Index: " + columnIndex);
//            System.out.println("Sort Direction: " + sortDirection);
//            column_inds.add(columnIndex);
//            order.add(isDescending);
//        }

//        int len = column_inds.size();
//        for (int i=0; i<len; i++) {
//            boolean flag = order.get(i);
//            int ind = column_inds.get(i);
//            if (flag) {
//                accumulatedRows.sort((a, b) -> ((String)b[ind]).compareTo((String)a[ind]));
//            } else if (!flag) {
//                accumulatedRows.sort((a, b) -> ((String)a[ind]).compareTo((String)b[ind]));
//            }
//
//        }

        Comparator<Object[]> hierarchicalComparator = (row1, row2) -> {
            for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
                int columnIndex = fieldCollation.getFieldIndex();
                Comparable value1 = (Comparable) row1[columnIndex];
                Comparable value2 = (Comparable) row2[columnIndex];

                int result;
                if (fieldCollation.getDirection().isDescending()) {
                    result = value2.compareTo(value1); // Compare in reverse for descending
                } else {
                    result = value1.compareTo(value2); // Compare normally for ascending
                }

                if (result != 0) {
                    return result;
                }
            }
            return 0;
        };

// Apply the hierarchical comparator to sort the accumulatedRows list
        accumulatedRows.sort(hierarchicalComparator);

        if (fetch != null) {
            limit = (int) ((RexLiteral) fetch).getValue2();
        } else {
            limit = size;
        }

        if (offset != null) {
            rows_to_skip = (int) ((RexLiteral) offset).getValue2();
        }
//        for (int i=0; i<accumulatedRows.size(); i++) {
//            Object[] r = accumulatedRows.get(i);
//            for (Object val: r) {
//                System.out.print(val + " ");
//            }
//            System.out.println();
//        }

        return true;

    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        /* Write your code here */
//        RelNode child = getInput();
//        PRel pRel = (PRel) child;
//        System.out.println("PSort hasnext");
//        return pRel.hasNext();
        while(index < rows_to_skip && index < size) {
            next();
        }

        if (index < size && limit_count < limit) {
            limit_count += 1;
            return true;
        }
        return false;

        //return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        /* Write your code here */
//        System.out.println("PSort Next");

        if (index == size) {
            return null;
        }
        Object[] row = accumulatedRows.get(index);

        index++;
        return row;
        // return null;
    }

}
