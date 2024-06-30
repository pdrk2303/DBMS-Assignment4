package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;

import java.util.*;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    private List<Object[]> accumulatedRows = new ArrayList<>();
    private int index = 0;
    private int size = 0;
    private int result_size = 0;
    private int row_size = 0;
    private List<Object[]> result = new ArrayList<>();
    private boolean flag = true;
    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        /* Write your code here */
//        System.out.println("Opening PAggregate");
//        System.out.println("Cluster: " + getCluster());
//        System.out.println("TraitSet: " + getTraitSet());
//        System.out.println("Hints: " + getHints());
//        System.out.println("Input: " + getInput());
//        System.out.println("GroupSet: " + getGroupSet());
//        System.out.println("GroupSets: " + getGroupSets());
//        System.out.println("AggCallList: " + getAggCallList());

        final RelDataType row_type = getRowType();
//        System.out.println("Row Type: " + row_type);
        List<RelDataTypeField> fields = row_type.getFieldList();

        // Create a list to store data types
        List<SqlTypeName> dataTypesList = new ArrayList<>();

        // Iterate over the fields to extract data types
        for (RelDataTypeField field : fields) {
            // Get the data type of each field
            SqlTypeName dataType = field.getType().getSqlTypeName();
            dataTypesList.add(dataType);
        }

        // Print the list of data types
//        System.out.println("Data Types List: " + dataTypesList);

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
//                System.out.println("Row printing: ");
                accumulatedRows.add(row);
//                System.out.println("Row is not NULL");
            }
            f = pRel.hasNext();
        }


//        for (int i=0; i<accumulatedRows.size(); i++) {
//            Object[] r = accumulatedRows.get(i);
//            for (Object val: r) {
//                System.out.print(val + " ");
//            }
//            System.out.println();
//        }
        size = accumulatedRows.size();
//        result_size = aggCalls.size();
        row_size = dataTypesList.size();
//        System.out.println("Got all rows: " + size);

        List<String> aggregate_funcs = new ArrayList();
        List<List<Integer>> aggregate_col_inds = new ArrayList();
        List<Boolean> distinct = new ArrayList<>();
        for (AggregateCall aggregateCall : aggCalls) {
            String functionName = aggregateCall.getAggregation().getName();
//            System.out.println("Aggregate Function: " + functionName);
            aggregate_funcs.add(functionName);

            List<Integer> columnIndices = new ArrayList<>();
            boolean isDistinct = aggregateCall.isDistinct();
//            System.out.println("is disntinct: " + isDistinct);
            distinct.add(isDistinct);
            for (Integer columnIndex : aggregateCall.getArgList()) {
//                System.out.println("Column Index: " + columnIndex);
                columnIndices.add(columnIndex);
            }
            aggregate_col_inds.add(columnIndices);

//            if (aggregateCall.getArgList().size() == 1) {
//                int columnIndex = aggregateCall.getArgList().get(0);
//                System.out.println("Column Index: " + columnIndex);
//                aggregate_col_inds.add(columnIndex);
//            } else {
//                aggregate_col_inds.add(-1);
//            }
        }

//        for (int i=0; i<size; i++) {
//            System.out.println(aggregate_funcs.get(i) + " : " + aggregate_col_inds.get(i));
//        }


        if (groupSet.isEmpty()) {
            Object[] new_row = new Object[row_size];
            for (int i=0; i< aggCalls.size(); i++) {
                String functionName = aggregate_funcs.get(i);
                List<Integer> l_ind = aggregate_col_inds.get(i);
                SqlTypeName dtype = dataTypesList.get(i);
                boolean isDistinct = distinct.get(i);
                switch (functionName) {
                    case "COUNT":
                        if (!l_ind.isEmpty()) {

                            Object cnt = null;
                            if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                cnt = 0;
                                if (isDistinct) {
                                    HashMap<List<Object>, Integer> hashMap = new HashMap<>();
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        boolean b = true;
                                        List<Object> l = new ArrayList<>();
                                        for (int ind : l_ind) {
                                            if (row[ind] == null) {
                                                b = false;
                                                break;
                                            } else {
                                                l.add(row[ind]);
                                            }
                                        }
                                        if (b) {
                                            if (! hashMap.containsKey(l)) {
                                                hashMap.put(l, 1);
                                                cnt = ((Integer) cnt) + 1;
                                            }
                                        }
                                    }
                                } else {
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        boolean b = true;
                                        for (int ind : l_ind) {
                                            if (row[ind] == null) {
                                                b = false;
                                                break;
                                            }
                                        }
                                        if (b) {
                                            cnt = ((Integer) cnt) + 1;
                                        }
                                    }
                                }

                            }
                            new_row[i] = cnt;
                        } else {

                            new_row[i] = size;
                        }
                        break;
                    case "SUM":

                        if (!l_ind.isEmpty()) {
                            int ind = l_ind.get(0);
                            Object cnt = null;
                            if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                cnt = 0;
                                if (isDistinct) {
                                    HashMap<Object, Integer> hashMap = new HashMap<>();
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        boolean b = true;
                                        if (row[ind] != null) {
                                            if (! hashMap.containsKey(row[ind])) {
                                                hashMap.put(row[ind], 1);
                                                cnt = ((Integer) cnt) + (Integer) row[ind];
                                            }
                                        }
                                    }
                                } else {
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            cnt = ((Integer) cnt) + (Integer) row[ind];
                                        }
                                    }
                                }

                            } else if (dtype == SqlTypeName.FLOAT) {
                                cnt = 0.0f;
                                if (isDistinct) {
                                    HashMap<Object, Integer> hashMap = new HashMap<>();
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {

                                            if (! hashMap.containsKey(row[ind])) {
                                                hashMap.put(row[ind], 1);
                                                cnt = (Float) cnt + (Float) row[ind];
                                            }
                                        }
                                    }
                                } else {
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            cnt = (Float) cnt + (Float) row[ind];;
                                        }
                                    }
                                }
                            } else if (dtype == SqlTypeName.DOUBLE) {
                                cnt = 0.0;
                                if (isDistinct) {
                                    HashMap<Object, Integer> hashMap = new HashMap<>();
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {

                                            if (! hashMap.containsKey(row[ind])) {
                                                hashMap.put(row[ind], 1);
                                                cnt = (Double) cnt + (Double) row[ind];
                                            }
                                        }
                                    }
                                } else {
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            cnt = (Double) cnt + (Double) row[ind];;
                                        }
                                    }
                                }

                            }
                            new_row[i] = cnt;
                        }
                        break;
                    case "MAX":

                        if (!l_ind.isEmpty()) {
                            int ind = l_ind.get(0);
                            Object max = null;
                            if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                max = Integer.MIN_VALUE;
                                for (int j=0; j<size; j++) {
                                    Object[] row = accumulatedRows.get(j);
                                    if (row[ind] != null) {
                                        if (((Integer) max).compareTo((Integer) row[ind]) < 0) {
                                            max = row[ind];
                                        }
                                    }
                                }
                            } else if (dtype == SqlTypeName.FLOAT) {
                                max = Float.MIN_VALUE;
                                for (int j=0; j<size; j++) {
                                    Object[] row = accumulatedRows.get(j);
                                    if (row[ind] != null) {
                                        if (((Float) max).compareTo((Float) row[ind]) < 0) {
                                            max = row[ind];
                                        }
                                    }
                                }
                            } else if (dtype == SqlTypeName.DOUBLE) {
                                max = Double.MIN_VALUE;
                                for (int j=0; j<size; j++) {
                                    Object[] row = accumulatedRows.get(j);
                                    if (row[ind] != null) {
                                        if (((Double) max).compareTo((Double) row[ind]) < 0) {
                                            max = row[ind];
                                        }
                                    }
                                }
                            }
                            new_row[i] = max;
                        }
                        break;
                    case "MIN":
                        if (!l_ind.isEmpty()) {
                            int ind = l_ind.get(0);
                            Object min = null;
                            if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                min = Integer.MAX_VALUE;
                                for (int j=0; j<size; j++) {
                                    Object[] row = accumulatedRows.get(j);
                                    if (row[ind] != null) {
                                        if (((Integer) min).compareTo((Integer) row[ind]) > 0) {
                                            min = row[ind];
                                        }
                                    }
                                }
                            } else if (dtype == SqlTypeName.FLOAT) {
                                min = Float.MAX_VALUE;
                                for (int j=0; j<size; j++) {
                                    Object[] row = accumulatedRows.get(j);
                                    if (row[ind] != null) {
                                        if (((Float) min).compareTo((Float) row[ind]) > 0) {
                                            min = row[ind];
                                        }
                                    }
                                }
                            } else if (dtype == SqlTypeName.DOUBLE) {
                                min = Double.MAX_VALUE;
                                for (int j=0; j<size; j++) {
                                    Object[] row = accumulatedRows.get(j);
                                    if (row[ind] != null) {
                                        if (((Double) min).compareTo((Double) row[ind]) > 0) {
                                            min = row[ind];
                                        }
                                    }
                                }
                            }
                            new_row[i] = min;
                        }
                        break;
                    case "AVG":
                        if (!l_ind.isEmpty()) {
                            int ind = l_ind.get(0);
                            Object sum = null;
                            int count = 0;
                            if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                sum = 0;
                                if (isDistinct) {
                                    HashMap<Object, Integer> hashMap = new HashMap<>();
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            if (! hashMap.containsKey(row[ind])) {
                                                hashMap.put(row[ind], 1);
                                                sum = (Integer) sum + (Integer) row[ind];
                                                count++;
                                            }
                                        }
                                    }
                                } else {
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            sum = (Integer) sum + (Integer) row[ind];
                                            count++;
                                        }
                                    }
                                }

                                if (count > 0) {
                                    sum = (Integer) sum / count;
                                }
                            } else if (dtype == SqlTypeName.FLOAT) {
                                sum = 0.0f;
                                if (isDistinct) {
                                    HashMap<Object, Integer> hashMap = new HashMap<>();
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            if (! hashMap.containsKey(row[ind])) {
                                                hashMap.put(row[ind], 1);
                                                sum = (Float) sum + (Float) row[ind];
                                                count++;
                                            }
                                        }
                                    }
                                } else {
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            sum = (Float) sum + (Float) row[ind];
                                            count++;
                                        }
                                    }
                                }

                                if (count > 0) {
                                    sum = (Float) sum / count;
                                }
                            } else if (dtype == SqlTypeName.DOUBLE) {
                                sum = 0.0;
                                if (isDistinct) {
                                    HashMap<Object, Integer> hashMap = new HashMap<>();
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            if (! hashMap.containsKey(row[ind])) {
                                                hashMap.put(row[ind], 1);
                                                sum = (Double) sum + (Double) row[ind];
                                                count++;
                                            }
                                        }
                                    }
                                } else {
                                    for (int j=0; j<size; j++) {
                                        Object[] row = accumulatedRows.get(j);
                                        if (row[ind] != null) {
                                            sum = (Double) sum + (Double) row[ind];
                                            count++;
                                        }
                                    }
                                }

                                if (count > 0) {
                                    sum = (Double) sum / count;
                                }
                            }
                            new_row[i] = sum;
                        }
                        break;
                }
            }
            if (new_row != null) {
                result.add(new_row);
            }

//            result_size = 1;
        } else {
//            System.out.println("Group By clause");
            List<Integer> valuesList = new ArrayList<>();

            for (Integer value : groupSet) {
                valuesList.add(value);
            }
//            System.out.println("Values list: " + valuesList);
            if (accumulatedRows.size() != 0) {
                int rsize = accumulatedRows.get(0).length;
//                if (rsize == valuesList.size()) {
//                    System.out.println("Thank you GOD!!!");
//                }

                HashMap<List<Object>, List<Object>> hashMap = new HashMap<>();
                List<HashMap<List<Object>, Integer>> hp_list = new ArrayList<>();
                HashMap<List<Object>, List<Integer>> hp_avg = new HashMap<>();
                HashMap<Integer, Integer> index_list = new HashMap<>();
                int avg_c = 0;
                for (int i=0; i< aggCalls.size(); i++) {
                    String functionName = aggregate_funcs.get(i);
                    HashMap<List<Object>, Integer> hp = new HashMap<>();
                    hp_list.add(hp);
                    if (Objects.equals(functionName, "AVG")) {
                        index_list.put(i, avg_c);
                        avg_c++;
                    }
                }
                int c = 0;
                int count = 0;

                for (int i=0; i<accumulatedRows.size(); i++) {

                    Object[] row = accumulatedRows.get(i);
                    Object[] r = new Object[valuesList.size()];

                    for (int k=0; k< valuesList.size(); k++) {
                        r[k] = row[valuesList.get(k)];
                    }
                    List<Object> list = Arrays.asList(r);

                    if (aggCalls.isEmpty()) {
                        if (! hashMap.containsKey(list)) {
                            List<Object> qq = new ArrayList<>();
                            qq.add(1);
                            hashMap.put(list, qq);
                        }
                    }

                    for (int j=0; j< aggCalls.size(); j++) {
                        String functionName = aggregate_funcs.get(j);
                        List<Integer> l_ind = aggregate_col_inds.get(j);
                        Boolean isDistinct = distinct.get(j);
                        HashMap<List<Object>, Integer> hp = hp_list.get(j);
//                        int start = rsize + j;
                        int start = valuesList.size() + j;
                        SqlTypeName dtype = dataTypesList.get(start);

                        switch (functionName) {
                            case "COUNT":

                                if (l_ind.isEmpty()) {
                                    if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                        if (hashMap.containsKey(list)) {

                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(0);
                                            }
                                            Object cnt = l.get(j);
                                            cnt = (Integer) cnt + 1;
                                            l.set(j, cnt);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            l.add(1);
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    }
                                } else {
//                                    int ind = aggregate_col_inds.get(j).get(0);
                                    if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                        if (hashMap.containsKey(list)) {

                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(0);
                                            }
                                            Object cnt = l.get(j);

                                            if (isDistinct) {
                                                List<Object> tt = new ArrayList<>();
                                                boolean b = true;
                                                for (int ind : l_ind) {
                                                    if (row[ind] == null) {
                                                        b = false;
                                                        break;
                                                    } else {
                                                        tt.add(row[ind]);
                                                    }
                                                }
                                                if (b) {
                                                    if (! hp.containsKey(tt)) {
                                                        hp.put(tt, 1);
                                                        cnt = ((Integer) cnt) + 1;
                                                    }
                                                }
                                            } else {
                                                boolean b = true;
                                                for (int ind : l_ind) {
                                                    if (row[ind] == null) {
                                                        b = false;
                                                        break;
                                                    }
                                                }
                                                if (b) {
                                                    cnt = ((Integer) cnt) + 1;
                                                }
                                            }

                                            l.set(j, cnt);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            boolean b = true;
                                            for (int ind : l_ind) {
                                                if (row[ind] == null) {
                                                    b = false;
                                                    break;
                                                }
                                            }
                                            if (b) {
                                                l.add(1);
                                            } else {
                                                l.add(0);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    }
                                }


                                break;
                            case "SUM":
//                                System.out.println("Entered SUM: " + l_ind);
                                if (!l_ind.isEmpty()) {
                                    int ind = l_ind.get(0);
                                    if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(0);
                                            }
                                            Object cnt = l.get(j);
                                            if (isDistinct) {
                                                if (row[ind] != null) {
                                                    List<Object> tt = new ArrayList<>();
                                                    tt.add(row[ind]);
                                                    if (! hp.containsKey(tt)) {
                                                        hp.put(tt, 1);
                                                        cnt = (Integer) cnt + (Integer) row[ind];
                                                    }
                                                }
                                            } else {
                                                if (row[ind] != null) {
                                                    cnt = (Integer) cnt + (Integer) row[ind];
                                                }
                                            }

                                            l.set(j, cnt);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object sum = 0;
                                            if (row[ind] != null) {
                                                sum = (Integer) sum + (Integer) row[ind];
                                                l.add(sum);
                                                List<Object> tt = new ArrayList<>();
                                                tt.add(row[ind]);
                                                hp.put(tt, 1);
                                            } else {
                                                l.add(sum);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    } else if (dtype == SqlTypeName.FLOAT) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(0.0f);
                                            }
                                            Object cnt = l.get(j);
                                            if (isDistinct) {
                                                if (row[ind] != null) {
                                                    List<Object> tt = new ArrayList<>();
                                                    tt.add(row[ind]);
                                                    if (! hp.containsKey(tt)) {
                                                        hp.put(tt, 1);
                                                        cnt = (Float) cnt + (Float) row[ind];
                                                    }
                                                }
                                            } else {
                                                if (row[ind] != null) {
                                                    cnt = (Float) cnt + (Float) row[ind];
                                                }
                                            }

                                            l.set(j, cnt);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object sum = 0;
                                            if (row[ind] != null) {
                                                sum = (Float) sum + (Float) row[ind];
                                                l.add(sum);
                                                List<Object> tt = new ArrayList<>();
                                                tt.add(row[ind]);
                                                hp.put(tt, 1);
                                            } else {
                                                l.add(sum);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    } else if (dtype == SqlTypeName.DOUBLE) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(0.0);
                                            }
                                            Object cnt = l.get(j);
                                            if (isDistinct) {
                                                if (row[ind] != null) {
                                                    List<Object> tt = new ArrayList<>();
                                                    tt.add(row[ind]);
                                                    if (! hp.containsKey(tt)) {
                                                        hp.put(tt, 1);
                                                        cnt = (Double) cnt + (Double) row[ind];
                                                    }
                                                }
                                            } else {
                                                if (row[ind] != null) {
                                                    cnt = (Double) cnt + (Double) row[ind];
                                                }
                                            }

                                            l.set(j, cnt);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object sum = 0.0;
                                            if (row[ind] != null) {
                                                sum = (Double) sum + (Double) row[ind];
                                                l.add(sum);
                                                List<Object> tt = new ArrayList<>();
                                                tt.add(row[ind]);
                                                hp.put(tt, 1);
                                            } else {
                                                l.add(sum);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    }
                                }
                                break;
                            case "MAX":
//                                System.out.println("Entered MAX");
                                if (!l_ind.isEmpty()) {
                                    int ind = l_ind.get(0);
                                    if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(Integer.MIN_VALUE);
                                            }
                                            Object max = l.get(j);
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) < 0) {
                                                    max = row[ind];
                                                }
                                            }
                                            l.set(j, max);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object max = Integer.MIN_VALUE;
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) < 0) {
                                                    max = row[ind];
                                                }
                                                l.add(max);
                                            } else {
                                                l.add(max);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    } else if (dtype == SqlTypeName.FLOAT) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(Float.MIN_VALUE);
                                            }
                                            Object max = l.get(j);
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) < 0) {
                                                    max = row[ind];
                                                }
                                            }
                                            l.set(j, max);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object max = Float.MIN_VALUE;
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) < 0) {
                                                    max = row[ind];
                                                }
                                                l.add(max);
                                            } else {
                                                l.add(max);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    } else if (dtype == SqlTypeName.DOUBLE) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(Double.MIN_VALUE);
                                            }
                                            Object max = l.get(j);
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) < 0) {
                                                    max = row[ind];
                                                }
                                            }
                                            l.set(j, max);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object max = Double.MIN_VALUE;
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) < 0) {
                                                    max = row[ind];
                                                }
                                                l.add(max);
                                            } else {
                                                l.add(max);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    }
                                }
                                break;
                            case "MIN":
//                                System.out.println("Entered MIN");
                                if (!l_ind.isEmpty()) {
                                    int ind = l_ind.get(0);
                                    if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(Integer.MAX_VALUE);
                                            }
                                            Object max = l.get(j);
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) > 0) {
                                                    max = row[ind];
                                                }
                                            }
                                            l.set(j, max);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object max = Integer.MAX_VALUE;
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) > 0) {
                                                    max = row[ind];
                                                }
                                                l.add(max);
                                            } else {
                                                l.add(max);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    } else if (dtype == SqlTypeName.FLOAT) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(Float.MAX_VALUE);
                                            }
                                            Object max = l.get(j);
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) > 0) {
                                                    max = row[ind];
                                                }
                                            }
                                            l.set(j, max);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object max = Float.MAX_VALUE;
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) > 0) {
                                                    max = row[ind];
                                                }
                                                l.add(max);
                                            } else {
                                                l.add(max);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    } else if (dtype == SqlTypeName.DOUBLE) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(Double.MAX_VALUE);
                                            }
                                            Object max = l.get(j);
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) > 0) {
                                                    max = row[ind];
                                                }
                                            }
                                            l.set(j, max);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object max = Double.MAX_VALUE;
                                            if (row[ind] != null) {
                                                if (((Integer) max).compareTo((Integer) row[ind]) > 0) {
                                                    max = row[ind];
                                                }
                                                l.add(max);
                                            } else {
                                                l.add(max);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    }
                                }
                                break;
                            case "AVG":
                                if (!l_ind.isEmpty()) {
                                    int avg_id = index_list.get(j);
                                    int ind = l_ind.get(0);
                                    if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(0);
                                            }
                                            Object cnt = l.get(j);

                                            if (isDistinct) {
                                                if (row[ind] != null) {
                                                    List<Object> tt = new ArrayList<>();
                                                    tt.add(row[ind]);
                                                    if (! hp.containsKey(tt)) {
                                                        hp.put(tt, 1);
                                                        List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                        if (avg_l.size() == avg_id) {
                                                            avg_l.add(0);
                                                        }
                                                        count = avg_l.get(avg_id);
                                                        cnt = ((Integer) cnt)*count + (Integer) row[ind];
                                                        count ++;
                                                        cnt = (Integer) cnt / count;
                                                        avg_l.set(avg_id, count);
                                                        hp_avg.put(list, avg_l);
                                                    }
                                                }
                                            } else {
                                                if (row[ind] != null) {
                                                    List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                    if (avg_l.size() == avg_id) {
                                                        avg_l.add(0);
                                                    }
                                                    count = avg_l.get(avg_id);
                                                    cnt = ((Integer) cnt)*count + (Integer) row[ind];
                                                    count ++;
                                                    cnt = (Integer) cnt / count;
                                                    avg_l.set(avg_id, count);
                                                    hp_avg.put(list, avg_l);
                                                }
                                            }

                                            l.set(j, cnt);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object sum = 0;
                                            if (row[ind] != null) {
                                                List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                if (avg_l.size() == avg_id) {
                                                    avg_l.add(0);
                                                }
                                                count = avg_l.get(avg_id);
                                                sum = ((Integer) sum)*count + (Integer) row[ind];
                                                count++;
                                                sum = (Integer) sum/count;
                                                l.add(sum);
                                                List<Object> tt = new ArrayList<>();
                                                tt.add(row[ind]);
                                                hp.put(tt, 1);
                                                avg_l.set(avg_id, count);
                                                hp_avg.put(list, avg_l);
                                            } else {
                                                l.add(sum);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    } else if (dtype == SqlTypeName.FLOAT) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(0.0f);
                                            }
                                            Object cnt = l.get(j);
                                            if (isDistinct) {
                                                if (row[ind] != null) {
                                                    List<Object> tt = new ArrayList<>();
                                                    tt.add(row[ind]);
                                                    if (! hp.containsKey(tt)) {
                                                        List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                        if (avg_l.size() == avg_id) {
                                                            avg_l.add(0);
                                                        }
                                                        count = avg_l.get(avg_id);
                                                        hp.put(tt, 1);
                                                        cnt = ((Float) cnt)*count + (Float) row[ind];
                                                        count ++;
                                                        cnt = (Float) cnt / count;
                                                        avg_l.set(avg_id, count);
                                                        hp_avg.put(list, avg_l);
                                                    }
                                                }
                                            } else {
                                                if (row[ind] != null) {
                                                    List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                    if (avg_l.size() == avg_id) {
                                                        avg_l.add(0);
                                                    }
                                                    count = avg_l.get(avg_id);
                                                    cnt = ((Float) cnt)*count + (Float) row[ind];
                                                    count ++;
                                                    cnt = (Float) cnt / count;
                                                    avg_l.set(avg_id, count);
                                                    hp_avg.put(list, avg_l);
                                                }
                                            }

                                            l.set(j, cnt);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object sum = 0.0f;
                                            if (row[ind] != null) {
                                                List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                if (avg_l.size() == avg_id) {
                                                    avg_l.add(0);
                                                }
                                                count = avg_l.get(avg_id);
                                                sum = ((Float) sum)*count + (Integer) row[ind];
                                                count++;
                                                sum = (Float) sum/count;
                                                l.add(sum);
                                                List<Object> tt = new ArrayList<>();
                                                tt.add(row[ind]);
                                                hp.put(tt, 1);
                                                avg_l.set(avg_id, count);
                                                hp_avg.put(list, avg_l);
                                            } else {
                                                l.add(sum);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    } else if (dtype == SqlTypeName.DOUBLE) {
                                        if (hashMap.containsKey(list)) {
                                            List<Object> l = hashMap.get(list);
                                            if (l.size() == j) {
                                                l.add(0.0);
                                            }
                                            Object cnt = l.get(j);
                                            if (isDistinct) {
                                                if (row[ind] != null) {
                                                    List<Object> tt = new ArrayList<>();
                                                    tt.add(row[ind]);
                                                    if (! hp.containsKey(tt)) {
                                                        List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                        if (avg_l.size() == avg_id) {
                                                            avg_l.add(0);
                                                        }
                                                        count = avg_l.get(avg_id);
                                                        hp.put(tt, 1);
                                                        cnt = ((Double) cnt)*count + (Double) row[ind];
                                                        count ++;
                                                        cnt = (Double) cnt / count;
                                                        avg_l.set(avg_id, count);
                                                        hp_avg.put(list, avg_l);
                                                    }
                                                }
                                            } else {
                                                if (row[ind] != null) {
                                                    List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                    if (avg_l.size() == avg_id) {
                                                        avg_l.add(0);
                                                    }
                                                    count = avg_l.get(avg_id);
                                                    cnt = ((Double) cnt)*count + (Double) row[ind];
                                                    count ++;
                                                    cnt = (Double) cnt / count;
                                                    avg_l.set(avg_id, count);
                                                    hp_avg.put(list, avg_l);
                                                }
                                            }

                                            l.set(j, cnt);
                                            hashMap.put(list, l);
                                        } else {
                                            List<Object> l = new ArrayList<>();
                                            Object sum = 0.0;
                                            if (row[ind] != null) {
                                                List<Integer> avg_l = hp_avg.getOrDefault(list, new ArrayList<>());
                                                if (avg_l.size() == avg_id) {
                                                    avg_l.add(0);
                                                }
                                                count = avg_l.get(avg_id);
                                                sum = ((Double) sum)*count + (Double) row[ind];
                                                count++;
                                                sum = (Double) sum/count;
                                                l.add(sum);
                                                List<Object> tt = new ArrayList<>();
                                                tt.add(row[ind]);
                                                hp.put(tt, 1);
                                                avg_l.set(avg_id, count);
                                                hp_avg.put(list, avg_l);
                                            } else {
                                                l.add(sum);
                                            }
                                            hashMap.put(list, l);
                                            c++;
                                        }
                                    }
                                }
                                break;
                        }

                    }
                }

//                System.out.println("---------rows in hashmap:  " + c);

                for (List<Object> key: hashMap.keySet()) {
                    Object[] row = new Object[row_size];
                    for (int j=0; j<valuesList.size(); j++) {
                        row[j] = key.get(j);
                    }
                    List<Object> value = hashMap.get(key);
                    for (int j=0; j<aggCalls.size(); j++) {
                        row[j + valuesList.size()] = value.get(j);
                    }
                    if (row != null) {
                        result.add(row);
                    }
                }

            }

        }


        result_size = result.size();
//        System.out.println("Result Size from aggregate: " + result_size);
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        /* Write your code here */
        if (index < result_size) {
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");

        if (index == result_size) {
            return null;
        }
        Object[] row = result.get(index);
        index++;
        return row;
    }

}