package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.*;

/*
 * Implement Hash Join
 * The left child is blocking, the right child is streaming
 */
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    private List<Object[]> accumulatedRows = new ArrayList<>();
    private int size = 0;
    private int index = 0;

    private Object operand_object(RexNode operand, Object[] row, int len) {
        if (operand instanceof RexInputRef) {
            int idx = ((RexInputRef) operand).getIndex();
            idx = idx - len;
            if (idx >= 0 && idx < row.length) {
                return row[idx];
            }
        }
        return null;
    }
    private Object[] evaluate(SqlOperator operator,List<RexNode> operands, Object[] row, int len, boolean flag) {
        SqlKind op_kind = operator.getKind();
        if (op_kind == SqlKind.AND) {
            Object[] a = evaluate(((RexCall) operands.get(0)).getOperator(), ((RexCall) operands.get(0)).getOperands(), row, len, flag);
            Object[] b = evaluate(((RexCall) operands.get(1)).getOperator(), ((RexCall) operands.get(1)).getOperands(), row, len, flag);
            if (a == null || b == null) {
                return null;
            }
            Object[] ans = new Object[a.length + b.length];
            for (int j=0; j<a.length; j++) {
                ans[j] = a[j];
            }
            for (int j=0; j< b.length; j++) {
                ans[a.length + j] = b[j];
            }
            return ans;
        } else if (op_kind == SqlKind.EQUALS) {
            Object a = operand_object(operands.get(0), row,0);
            Object b = operand_object(operands.get(1), row, len);

            Object[] ans = new Object[1];
            if (flag) {
                ans[0] = a;
            } else {
                ans[0] = b;
            }
            return ans;
        }
        return null;
    }
    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        /* Write your code here */

//        System.out.println("Opening PJoin");
//        System.out.println("Left input: " + getLeft());
//        System.out.println("Right input: " + getRight());
//        System.out.println("Join condition: " + getCondition());
//        System.out.println("Join type: " + getJoinType());

        PRel p_left = (PRel) left;
        PRel p_right = (PRel) right;

        boolean p1 = p_left.open();
        boolean p2 = p_right.open();
        if (!(p1 && p2)) {
            return false;
        }

        JoinRelType join_type = getJoinType();
        SqlOperator op = ((RexCall) condition).getOperator();
        List<RexNode> ops = ((RexCall) condition).getOperands();
        HashMap<List<Object>, List<Object[]>> hashMap = new HashMap<>();

        switch (join_type) {
            case LEFT:
//                System.out.println("LEFT JOIN");
                int left_len = 0;
                int right_len = 0;
                Object[] right_row = null;
//                int flag = 0;
                if (p_right.hasNext()) {
//                    flag = 1;
                    right_row = p_right.next();
                    right_len = right_row.length;
                }
                boolean a = p_left.hasNext();
                while (a) {
                    Object[] r = p_left.next();
                    left_len = r.length;
                    Object[] key = evaluate(op, ops, r, right_len, false);
                    if (key == null) {
                        a = p_left.hasNext();
                        continue;
                    }
                    List<Object> list = Arrays.asList(key);
                    if (hashMap.containsKey(list)) {
                        List<Object[]> l = hashMap.get(list);
                        l.add(r);
                        hashMap.put(list, l);
                    } else {
                        List<Object[]> l = new ArrayList<>();
                        l.add(r);
                        hashMap.put(list, l);
                    }
                    a = p_left.hasNext();
                }

                while (right_row != null || p_right.hasNext()) {
                    Object[] r_row;
                    if (right_row != null) {
                        r_row = right_row;
                    } else {
                        r_row = p_right.next();
                    }

                    Object[] key = evaluate(op, ops, r_row, left_len, true);
                    if (key == null) {
                        continue;
                    }
                    List<Object> list = Arrays.asList(key);
                    int present = 0;
                    if (hashMap.containsKey(list)) {
                        List<Object[]> l = hashMap.get(list);
                        present = 1;
                        for (int i=0; i<l.size(); i++) {
                            Object[] l_row = l.get(i);
                            int rsize = l_row.length + r_row.length;
                            Object[] new_row = new Object[rsize];
                            for (int j=0; j<l_row.length; j++) {
                                new_row[j] = l_row[j];
                            }
                            for (int j=0; j<r_row.length; j++) {
                                new_row[l_row.length + j] = r_row[j];
                            }
                            accumulatedRows.add(new_row);
                        }
                    }
                    if (present == 0) {
                        int rsize = left_len + r_row.length;
                        Object[] new_row = new Object[rsize];
                        for (int j=0; j<left_len; j++) {
                            new_row[j] = null;
                        }
                        for (int j=0; j< r_row.length; j++) {
                            new_row[left_len + j] = r_row[j];
                        }

                        accumulatedRows.add(new_row);
                    }
                }

                break;
            case RIGHT:
//                System.out.println("RIGHT JOIN");

                boolean f = p_left.hasNext();
                int r_len = 0;

                while (f) {
                    Object[] r = p_left.next();
                    r_len = r.length;
                    Object[] key = evaluate(op, ops, r, r_len, true);
                    if (key == null) {
                        f = p_left.hasNext();
                        continue;
                    }
                    List<Object> list = Arrays.asList(key);
                    if (hashMap.containsKey(list)) {
                        List<Object[]> l = hashMap.get(list);
                        l.add(r);
                        hashMap.put(list, l);
                    } else {
                        List<Object[]> l = new ArrayList<>();
                        l.add(r);
                        hashMap.put(list, l);
                    }
                    f = p_left.hasNext();
                }

                boolean b = p_right.hasNext();
                while (b) {
                    Object[] r_row = p_right.next();
                    Object[] key = evaluate(op, ops, r_row, r_len, false);
                    if (key == null) {
                        b = p_right.hasNext();
                        continue;
                    }
                    List<Object> list = Arrays.asList(key);
                    int present = 0;
                    if (hashMap.containsKey(list)) {
                        List<Object[]> l = hashMap.get(list);
                        present = 1;
                        for (int i=0; i<l.size(); i++) {

                            Object[] l_row = l.get(i);
                            int rsize = l_row.length + r_row.length;
                            Object[] new_row = new Object[rsize];
                            for (int j=0; j< l_row.length; j++) {
                                new_row[j] = l_row[j];
                            }
                            for (int j=0; j<r_row.length; j++) {
                                new_row[l_row.length + j] = r_row[j];
                            }
                            accumulatedRows.add(new_row);
                        }
                    }

                    if (present == 0) {
                        int rsize = r_len + r_row.length;
                        Object[] new_row = new Object[rsize];
                        for (int j=0; j<r_len; j++) {
                            new_row[j] = null;
                        }
                        for (int j=0; j< r_row.length; j++) {
                            new_row[r_len + j] = r_row[j];
                        }
                        accumulatedRows.add(new_row);
                    }
                    b = p_right.hasNext();
                }
                break;

            case INNER:
//                System.out.println("INNER JOIN");

                f = p_left.hasNext();
                r_len = 0;
                while (f) {
                    Object[] r = p_left.next();
                    r_len = r.length;
                    Object[] key = evaluate(op, ops, r, r_len, true);
                    if (key == null) {
                        f = p_left.hasNext();
                        continue;
                    }
                    List<Object> list = Arrays.asList(key);

                    if (hashMap.containsKey(list)) {
                        List<Object[]> l = hashMap.get(list);
                        l.add(r);
                        hashMap.put(list, l);
                    } else {
                        List<Object[]> l = new ArrayList<>();
                        l.add(r);
                        hashMap.put(list, l);
                    }
                    f = p_left.hasNext();
                }

                b = p_right.hasNext();
                while (b) {
                    Object[] r_row = p_right.next();
                    Object[] key = evaluate(op, ops, r_row, r_len, false);
                    if (key == null) {
                        b = p_right.hasNext();
                        continue;
                    }
                    List<Object> list = Arrays.asList(key);
                    if (hashMap.containsKey(list)) {
                        List<Object[]> l = hashMap.get(list);
                        for (int i=0; i<l.size(); i++) {

                            Object[] l_row = l.get(i);
                            int rsize = l_row.length + r_row.length;
                            Object[] new_row = new Object[rsize];
                            for (int j=0; j<l_row.length; j++) {
                                new_row[j] = l_row[j];
                            }
                            for (int j=0; j<r_row.length; j++) {
                                new_row[l_row.length + j] = r_row[j];
                            }

                            accumulatedRows.add(new_row);
                        }
                    }
                    b = p_right.hasNext();
                }
                break;
            case FULL:
                HashMap<List<Object>, List<Object[]>> hashMap2 = new HashMap<>();
                left_len = 0;
                right_len = 0;

                List<Object[]> left_rows = new ArrayList<>();
                boolean d = p_left.hasNext();
                while (d) {
                    Object[] r = p_left.next();
                    left_len = r.length;
                    left_rows.add(r);
                    Object[] key = evaluate(op, ops, r, left_len, true);
                    if (key == null) {
                        d = p_left.hasNext();
                        continue;
                    }
                    List<Object> list = Arrays.asList(key);
                    if (hashMap.containsKey(list)) {
                        List<Object[]> l = hashMap.get(list);
                        l.add(r);
                        hashMap.put(list, l);
                    } else {
                        List<Object[]> l = new ArrayList<>();
                        l.add(r);
                        hashMap.put(list, l);
                    }
                    d = p_left.hasNext();

                }

                boolean e = p_right.hasNext();
                while (e) {
                    Object[] r_row = p_right.next();
                    right_len = r_row.length;
                    Object[] key = evaluate(op, ops, r_row, left_len, false);
                    if (key == null) {
                        e = p_right.hasNext();
                        continue;
                    }
                    List<Object> list = Arrays.asList(key);
                    if (hashMap2.containsKey(list)) {
                        List<Object[]> l = hashMap2.get(list);
                        l.add(r_row);
                        hashMap2.put(list, l);
                    } else {
                        List<Object[]> l = new ArrayList<>();
                        l.add(r_row);
                        hashMap2.put(list, l);
                    }

                    int present = 0;
                    if (hashMap.containsKey(list)) {
                        List<Object[]> l = hashMap.get(list);
                        present = 1;
                        for (int i=0; i<l.size(); i++) {
                            Object[] l_row = l.get(i);
                            int rsize = l_row.length + r_row.length;
                            Object[] new_row = new Object[rsize];
                            for (int j=0; j< l_row.length; j++) {
                                new_row[j] = l_row[j];
                            }
                            for (int j=0; j<r_row.length; j++) {
                                new_row[l_row.length + j] = r_row[j];
                            }
                            accumulatedRows.add(new_row);
                        }
                    }

                    if (present == 0) {
                        int rsize = left_len + r_row.length;
                        Object[] new_row = new Object[rsize];
                        for (int j=0; j<left_len; j++) {
                            new_row[j] = null;
                        }
                        for (int j=0; j< r_row.length; j++) {
                            new_row[left_len + j] = r_row[j];
                        }
                        accumulatedRows.add(new_row);
                    }

                    for (Object[] left_row: left_rows) {
                        Object[] key1 = evaluate(op, ops, left_row, left_len, true);
                        List<Object> list1 = Arrays.asList(key1);
                        if (! hashMap2.containsKey(list1)) {
                            int rsize = left_row.length + right_len;
                            Object[] new_row = new Object[rsize];
                            for (int j=0; j<left_row.length; j++) {
                                new_row[j] = left_row[j];
                            }
                            for (int j=0; j<right_len; j++) {
                                new_row[left_row.length + j] = null;
                            }
                            accumulatedRows.add(new_row);
                        }
                    }

                    e = p_right.hasNext();
                }

                break;
        }
        size = accumulatedRows.size();
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        if (index < size) {
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        if (index == size) {
            return null;
        }
        Object[] row = accumulatedRows.get(index);
        index++;
        return row;
//        return null;
    }
}
