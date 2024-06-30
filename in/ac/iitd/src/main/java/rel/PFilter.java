package rel;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.List;


public class PFilter extends Filter implements PRel {

    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        /* Write your code here */
//        System.out.println("PFilter relNode is open: " + getInput());
        RelNode child = getInput();
        PRel pRel = (PRel) child;
        return pRel.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        /* Write your code here */
        return;
    }

    private Object handleInputRef(RexInputRef op, Object[] row) {
        int id = op.getIndex();
        if (id >= 0 && id < row.length) {
            return row[id];
        } else {
            throw new IllegalArgumentException("Invalid column index: " + id);
        }
    }

    private Object handleArithmetic(RexCall op, Object[] row) {
        SqlOperator operator = op.getOperator();
        List<RexNode> operands = op.getOperands();
        SqlKind op_kind = operator.getKind();
        Object a = resultant_operand(operands.get(0), row);
        Object b = resultant_operand(operands.get(1), row);
        if (a == null || b == null) {
            return null;
        }
        if (op_kind == SqlKind.PLUS) {
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.add(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.add(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.add(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.add(right);
                }
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.add(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.add(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.add(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.add(right);
                }
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.add(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.add(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.add(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.add(right);
                }
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.add(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.add(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.add(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.add(right);
                }
            }
        } else if (op_kind == SqlKind.MINUS) {
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.subtract(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.subtract(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.subtract(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.subtract(right);
                }
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.subtract(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.subtract(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.subtract(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.subtract(right);
                }
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.subtract(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.subtract(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.subtract(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.subtract(right);
                }
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.subtract(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.subtract(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.subtract(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.subtract(right);
                }
            }
        } else if (op_kind == SqlKind.TIMES) {
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.multiply(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.multiply(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.multiply(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.multiply(right);
                }
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.multiply(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.multiply(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.multiply(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.multiply(right);
                }
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.multiply(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.multiply(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.multiply(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.multiply(right);
                }
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.multiply(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.multiply(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.multiply(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.multiply(right);
                }
            }
        } else if (op_kind == SqlKind.DIVIDE) {
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.divide(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.divide(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.divide(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.divide(right);
                }
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.divide(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.divide(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.divide(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.divide(right);
                }
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.divide(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.divide(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.divide(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.divide(right);
                }
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                if (b instanceof Integer) {
                    BigDecimal right = new BigDecimal((Integer) b);
                    return left.divide(right);
                } else if (b instanceof Float) {
                    BigDecimal right = new BigDecimal((Float) b);
                    return left.divide(right);
                } else if (b instanceof Double) {
                    BigDecimal right = new BigDecimal((Double) b);
                    return left.divide(right);
                } else if (b instanceof BigDecimal) {
                    BigDecimal right = (BigDecimal) b;
                    return left.divide(right);
                }
            }
        }
        return null;
    }

    private Object resultant_operand(RexNode op, Object[] row) {
        if (op instanceof RexInputRef) {
            return handleInputRef((RexInputRef) op, row);
        } else if (op instanceof RexLiteral) {
            return ((RexLiteral) op).getValue();
        } else {
            return handleArithmetic((RexCall) op, row);
        }
    }

    private boolean result(SqlOperator op, List<RexNode> ops, Object[] row) {
        SqlKind op_kind = op.getKind();
        if (op_kind == SqlKind.NOT) {
            if (ops.size() == 1) {
                RexCall cond = (RexCall) ops.get(0);
                return !(result(cond.getOperator(), cond.getOperands(), row));
            }
        } else if (op_kind == SqlKind.AND) {
            boolean f = true;
            for (int i=0; i<ops.size(); i++) {
                RexCall cond = (RexCall) ops.get(i);
                if (! result(cond.getOperator(), cond.getOperands(), row)) {
                    f = false;
                    break;
                }
            }
            if (f) {
                return true;
            } else {
                return false;
            }
        } else if (op_kind == SqlKind.OR) {
            boolean f = false;
            for (int i=0; i<ops.size(); i++) {
                RexCall cond = (RexCall) ops.get(i);
                if (result(cond.getOperator(), cond.getOperands(), row)) {
                    f = true;
                    break;
                }
            }
            if (f) {
                return true;
            } else {
                return false;
            }
        } else if (op_kind == SqlKind.EQUALS) {
            Object a = resultant_operand(ops.get(0), row);
            Object b = resultant_operand(ops.get(1), row);
            if (a == null || b == null) {
                return false;
            }
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                int res = ((Comparable) left).compareTo(b);
                return res==0;
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                int res = ((Comparable) left).compareTo(b);
                return res==0;
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                int res = ((Comparable) left).compareTo(b);
                return res==0;
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                int res = ((Comparable) left).compareTo(b);
                return res==0;
            } else if (a instanceof Boolean) {
                Boolean left = (Boolean) a;
                int res = Boolean.compare(left, (Boolean) b);
                return res==0;
            } else if (a instanceof String) {
                String left = (String) a;
                NlsString nls = (NlsString) b;
                String right = nls.getValue();
                int res = ((Comparable) left).compareTo(right);
                return res==0;
            }
        } else if (op_kind == SqlKind.GREATER_THAN) {
            Object a = resultant_operand(ops.get(0), row);
            Object b = resultant_operand(ops.get(1), row);
            if (a == null || b == null) {
                return false;
            }
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                int res = ((Comparable) left).compareTo(b);
                return res>0;
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                int res = ((Comparable) left).compareTo(b);
                return res>0;
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                int res = ((Comparable) left).compareTo(b);
                return res>0;
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                int res = ((Comparable) left).compareTo(b);
                return res>0;
            } else if (a instanceof Boolean) {
                Boolean left = (Boolean) a;
                int res = Boolean.compare(left, (Boolean) b);
                return res>0;
            } else if (a instanceof String) {
                String left = (String) a;
                NlsString nls = (NlsString) b;
                String right = nls.getValue();
                int res = ((Comparable) left).compareTo(right);
                return res>0;
            }
        } else if (op_kind == SqlKind.GREATER_THAN_OR_EQUAL) {
            Object a = resultant_operand(ops.get(0), row);
            Object b = resultant_operand(ops.get(1), row);
            if (a == null || b == null) {
                return false;
            }
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                int res = ((Comparable) left).compareTo(b);
                return res >= 0;
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                int res = ((Comparable) left).compareTo(b);
                return res >= 0;
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                int res = ((Comparable) left).compareTo(b);
                return res >= 0;
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                int res = ((Comparable) left).compareTo(b);
                return res >= 0;
            } else if (a instanceof Boolean) {
                Boolean left = (Boolean) a;
                int res = Boolean.compare(left, (Boolean) b);
                return res >= 0;
            } else if (a instanceof String) {
                String left = (String) a;
                NlsString nls = (NlsString) b;
                String right = nls.getValue();
                int res = ((Comparable) left).compareTo(right);
                return res >= 0;
            }
        } else if (op_kind == SqlKind.LESS_THAN) {
            Object a = resultant_operand(ops.get(0), row);
            Object b = resultant_operand(ops.get(1), row);
            if (a == null || b == null) {
                return false;
            }
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                int res = ((Comparable) left).compareTo(b);
                return res<0;
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                int res = ((Comparable) left).compareTo(b);
                return res<0;
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                int res = ((Comparable) left).compareTo(b);
                return res<0;
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                int res = ((Comparable) left).compareTo(b);
                return res<0;
            } else if (a instanceof Boolean) {
                Boolean left = (Boolean) a;
                int res = Boolean.compare(left, (Boolean) b);
                return res<0;
            } else if (a instanceof String) {
                String left = (String) a;
                NlsString nls = (NlsString) b;
                String right = nls.getValue();
                int res = ((Comparable) left).compareTo(right);
                return res<0;
            }
        } else if (op_kind == SqlKind.LESS_THAN_OR_EQUAL) {
            Object a = resultant_operand(ops.get(0), row);
            Object b = resultant_operand(ops.get(1), row);
            if (a == null || b == null) {
                return false;
            }
            if (a instanceof Integer) {
                BigDecimal left = new BigDecimal((Integer) a);
                int res = ((Comparable) left).compareTo(b);
                return res<=0;
            } else if (a instanceof Float) {
                BigDecimal left = new BigDecimal((Float) a);
                int res = ((Comparable) left).compareTo(b);
                return res<=0;
            } else if (a instanceof Double) {
                BigDecimal left = new BigDecimal((Double) a);
                int res = ((Comparable) left).compareTo(b);
                return res<=0;
            } else if (a instanceof BigDecimal) {
                BigDecimal left = (BigDecimal) a;
                int res = ((Comparable) left).compareTo(b);
                return res<=0;
            } else if (a instanceof Boolean) {
                Boolean left = (Boolean) a;
                int res = Boolean.compare(left, (Boolean) b);
                return res<=0;
            } else if (a instanceof String) {
                String left = (String) a;
                NlsString nls = (NlsString) b;
                String right = nls.getValue();
                int res = ((Comparable) left).compareTo(right);
                return res<=0;
            }
        }
        return false;
    }

    private Object[] current_row;
    private int has_current_row = 0;
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
        /* Write your code here */
        RelNode child = getInput();
        PRel pRel = (PRel) child;
//        System.out.println("PFilter hasnext: " + pRel);
        boolean flag = pRel.hasNext();
        RexNode condition = getCondition();
        RexCall call = (RexCall) condition;
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();

        while (flag && has_current_row == 0) {
            Object[] row = pRel.next();
            while(row == null) {
                if (pRel.hasNext()) {
                    row = pRel.next();
                } else {
                    return false;
                }
            }

            boolean res = result(operator, operands, row);
            if (res) {
                has_current_row = 1;
                current_row = row;
                return true;
            }
            flag = pRel.hasNext();
        }
//        if (flag) {
//            Object[] row = pRel.next();
//            while(row == null) {
//                if (pRel.hasNext()) {
//                    row = pRel.next();
//                } else {
//                    return false;
//                }
//            }
//            has_current_row = 1;
//            current_row = row;
//            return true;
//        }
        return false;
//        return true;
    }

    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        /* Write your code here */
//        System.out.println("PFilter Next");

//        RelNode child = getInput();
//        PRel pRel = (PRel) child;
//        if (!pRel.hasNext()) {
//            return null;
//        }
//        Object[] row = pRel.next();
//        if (row == null) {
//            return null;
//        }

        Object[] row;
        if (has_current_row == 1) {
            has_current_row = 0;
            row = current_row;
        } else {
            RelNode child = getInput();
            PRel pRel = (PRel) child;
            row = pRel.next();
        }

        if (row == null) {
            return null;
        }

        return row;

//        RexNode condition = getCondition();
//        RexCall call = (RexCall) condition;
//        SqlOperator operator = call.getOperator();
//        List<RexNode> operands = call.getOperands();
//
//        boolean result = evaluate(operator, operands, row);
//        System.out.println("Evaluate return");
//        if (result) {
//            for (Object value: row){
//                System.out.print(value);
//                System.out.print(" ");
//            }
//            System.out.println();
//            return row;
//        } else {
////            System.out.println("NULL : row not selected");
//            return null;
//        }
    }
}
