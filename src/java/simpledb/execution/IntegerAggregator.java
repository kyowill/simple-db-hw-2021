package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;

    private Type gbFieldType;

    private int aField;

    private Op what;

    private Map<Field, List<Tuple>> groupMap;

    private static IntField NO_GROUP_FIELD = new IntField(-1);

    private static String GROUP_ITEM = "GROUP_ITEM";

    private static String AGGREGATE_ITEM = "AGGREGATE_ITEM";

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        this.what = what;
        groupMap = new HashMap<>();
        if (gbField == Aggregator.NO_GROUPING) {
            groupMap.put(NO_GROUP_FIELD, new ArrayList<>());
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // init tuple desc
        if (gbField == Aggregator.NO_GROUPING) {
            groupMap.get(NO_GROUP_FIELD).add(tup);
        } else {
            Field key = tup.getField(gbField);
            if (!groupMap.containsKey(key)) {
                groupMap.put(key, new ArrayList<>());
            }
            groupMap.get(key).add(tup);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        TupleDesc tupleDesc = constructTupleDesc();
        if (gbField == Aggregator.NO_GROUPING) {
            return constructNoGroupTuples(what, tupleDesc);
        }
        return constructGroupTuples(what, tupleDesc);
    }

    private TupleDesc constructTupleDesc() {
        TupleDesc tupleDesc = new TupleDesc();
        List<TupleDesc.TDItem> items = new ArrayList<>();
        if (gbField != Aggregator.NO_GROUPING) {
            TupleDesc.TDItem groupItem = new TupleDesc.TDItem(gbFieldType, GROUP_ITEM);
            items.add(groupItem);
        }
        TupleDesc.TDItem aggregateItem = new TupleDesc.TDItem(Type.INT_TYPE, AGGREGATE_ITEM);
        items.add(aggregateItem);
        tupleDesc.setTDItems(items);
        return tupleDesc;
    }

    private TupleIterator constructNoGroupTuples(Op what, TupleDesc tupleDesc) {
        List<Tuple> tuples = new ArrayList<>();
        List<Tuple> tupleList = groupMap.get(NO_GROUP_FIELD);
        Field result = null;
        if (what.name().equals(Op.MIN.name())) {
            int min = calMinVal(tupleList);
            result = new IntField(min);
        } else if (what.name().equals(Op.MAX.name())) {
            int max = calMaxVal(tupleList);
            result = new IntField(max);
        } else if (what.name().equals(Op.SUM.name())) {
            int sum = calSumVal(tupleList);
            result = new IntField(sum);
        } else if (what.name().equals(Op.AVG.name())) {
            int avg = calAvgVal(tupleList);
            result = new IntField(avg);
        } else if (what.name().equals(Op.COUNT.name())) {
            int cnt = calCountVal(tupleList);
            result = new IntField(cnt);
        } else {
            throw new RuntimeException(what.name() + ":not defined");
        }
        Tuple ele = new Tuple(tupleDesc);
        ele.setField(0, result);
        tuples.add(ele);
        TupleIterator tupleIterator = new TupleIterator(tupleDesc, tuples);
        return tupleIterator;
    }

    private TupleIterator constructGroupTuples(Op what, TupleDesc tupleDesc) {
        List<Tuple> tuples = new ArrayList<>();
        if (what.name().equals(Op.MIN.name())) {
            for (Map.Entry<Field, List<Tuple>> entry : groupMap.entrySet()) {
                Field groupKey = entry.getKey();
                List<Tuple> tupleList = entry.getValue();
                int min = calMinVal(tupleList);
                Tuple ele = new Tuple(tupleDesc);
                ele.setField(0, groupKey);
                ele.setField(1, new IntField(min));
                tuples.add(ele);
            }
        } else if (what.name().equals(Op.MAX.name())) {
            for (Map.Entry<Field, List<Tuple>> entry : groupMap.entrySet()) {
                Field groupKey = entry.getKey();
                List<Tuple> tupleList = entry.getValue();
                int max = calMaxVal(tupleList);
                Tuple ele = new Tuple(tupleDesc);
                ele.setField(0, groupKey);
                ele.setField(1, new IntField(max));
                tuples.add(ele);
            }
        } else if (what.name().equals(Op.SUM.name())) {
            for (Map.Entry<Field, List<Tuple>> entry : groupMap.entrySet()) {
                Field groupKey = entry.getKey();
                List<Tuple> tupleList = entry.getValue();
                int sum = calSumVal(tupleList);
                Tuple ele = new Tuple(tupleDesc);
                ele.setField(0, groupKey);
                ele.setField(1, new IntField(sum));
                tuples.add(ele);
            }
        } else if (what.name().equals(Op.AVG.name())) {
            for (Map.Entry<Field, List<Tuple>> entry : groupMap.entrySet()) {
                Field groupKey = entry.getKey();
                List<Tuple> tupleList = entry.getValue();
                int avg = calAvgVal(tupleList);
                Tuple ele = new Tuple(tupleDesc);
                ele.setField(0, groupKey);
                ele.setField(1, new IntField(avg));
                tuples.add(ele);
            }
        } else if (what.name().equals(Op.COUNT.name())) {
            for (Map.Entry<Field, List<Tuple>> entry : groupMap.entrySet()) {
                Field groupKey = entry.getKey();
                List<Tuple> tupleList = entry.getValue();
                int cnt = calCountVal(tupleList);
                Tuple ele = new Tuple(tupleDesc);
                ele.setField(0, groupKey);
                ele.setField(1, new IntField(cnt));
                tuples.add(ele);
            }
        } else {
            throw new RuntimeException(what.name() + ":not defined");
        }
        return new TupleIterator(tupleDesc, tuples);
    }

    private int calMinVal(List<Tuple> tupleList) {
        int min = Integer.MAX_VALUE;
        for (Tuple tuple : tupleList) {
            IntField intField = (IntField) tuple.getField(aField);
            min = Math.min(min, intField.getValue());
        }
        return min;
    }

    private int calMaxVal(List<Tuple> tupleList) {
        int max = Integer.MIN_VALUE;
        for (Tuple tuple : tupleList) {
            IntField intField = (IntField) tuple.getField(aField);
            max = Math.max(max, intField.getValue());
        }
        return max;
    }

    private int calSumVal(List<Tuple> tupleList) {
        int sum = 0;
        for (Tuple tuple : tupleList) {
            IntField intField = (IntField) tuple.getField(aField);
            sum += intField.getValue();
        }
        return sum;
    }

    private int calAvgVal(List<Tuple> tupleList) {
        int sum = 0;
        for (Tuple tuple : tupleList) {
            IntField intField = (IntField) tuple.getField(aField);
            sum += intField.getValue();
        }
        int avg = sum / tupleList.size();
        return avg;
    }

    private int calCountVal(List<Tuple> tupleList) {
        return tupleList.size();
    }
}
