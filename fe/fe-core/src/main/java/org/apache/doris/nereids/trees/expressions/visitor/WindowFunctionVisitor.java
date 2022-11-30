package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;

public interface WindowFunctionVisitor<R, C> {

    R visitWindowFunction(WindowFunction windowFunction, C context);

    default public R visitDenseRank(DenseRank denseRank, C context) {
        return visitWindowFunction(denseRank, context);
    }

    default public R visitFirstValue(FirstValue firstValue, C context) {
        return visitWindowFunction(firstValue, context);
    }

    default public R visitLag(Lag lag, C context) {
        return visitWindowFunction(lag, context);
    }

    default public R visitLastValue(LastValue lastValue, C context) {
        return visitWindowFunction(lastValue, context);
    }

    default public R visitLead(Lead lead, C context) {
        return visitWindowFunction(lead, context);
    }

    default public R visitRank(Rank rank, C context) {
        return visitWindowFunction(rank, context);
    }

    default public R visitRowNumber(RowNumber rowNumber, C context) {
        return visitWindowFunction(rowNumber, context);
    }
}
