package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstValue;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.Optional;

/**
 * todo: the return type may be Void?
 */
public class WindowFunctionChecker extends ExpressionVisitor<Expression, WindowFunctionChecker.CheckContext> {

    private Window window;

    public WindowFunctionChecker(Window window) {
        this.window = window;
    }

    public WindowFunction check() {
        // todo: change Window.windowFunction's class from UnboundFunction to WindowFunction
        Expression windowFunction = window.getWindowFunction();
        return (WindowFunction)windowFunction.accept(this, new CheckContext());
    }

    @Override
    public WindowFunction visit(Expression expr, CheckContext context) {
        return null;
    }

    @Override
    public RowNumber visitRowNumber(RowNumber rowNumber, CheckContext ctx) {
        // check and complete window frame
        Optional<WindowFrame> windowFrame = window.getWindowSpec().getWindowFrame();
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
            FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        if (windowFrame.isPresent()) {
            if (!windowFrame.get().equals(requiredFrame)) {
                throw new AnalysisException("WindowFrame for ROW_NUMBER() must be null " +
                    "or match with (ROWS, UNBOUNDED PRECEDING, CURRENT ROW");
            }
        } else {
            window.getWindowSpec().setWindowFrame(requiredFrame);
        }

        // todo: should OrderKey be required?
        return rowNumber;
    }

    @Override
    public Lag visitLag(Lag lag, CheckContext ctx) {
        return null;
    }

    @Override
    public Lead visitLead(Lead lead, CheckContext ctx) {
        return null;
    }

    @Override
    public FirstValue visitFirstValue(FirstValue firstValue, CheckContext ctx) {
        return null;
    }

    @Override
    public Rank visitRank(Rank rank, CheckContext ctx) {
        return null;
    }

    @Override
    public DenseRank visitDenseRank(DenseRank denseRank, CheckContext ctx) {
        return null;
    }

    public static class CheckContext {

    }
}
