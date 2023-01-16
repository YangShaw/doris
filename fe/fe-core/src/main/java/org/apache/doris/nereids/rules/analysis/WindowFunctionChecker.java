// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstOrLastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstValue;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * todo: the return type may be Void?
 */
public class WindowFunctionChecker extends DefaultExpressionVisitor<Expression, WindowFunctionChecker.CheckContext> {

    private Window window;

    public WindowFunctionChecker(Window window) {
        this.window = window;
    }

    public Window getWindow() {
        return window;
    }

    /**
     * step 1: check windowFrame in window;
     */
    public void checkWindowFrameBeforeFunc() {
        window.getWindowFrame().ifPresent(this::checkWindowFrame);
    }

    /**
     * step 2: check windowFunction in window
     */
    public Expression checkWindowFunction() {
        // todo: visitNtile()

        // in checkWindowFrameBeforeFunc() we have confirmed that both left and right boundary are set as long as
        // windowFrame exists, therefore in all following visitXXX functions we don't need to check whether the right
        // boundary is null.
        return window.accept(this, new CheckContext());
    }

    /**
     * step 3: check window
     */
    public void checkWindowAfterFunc() {
        // reverse windowFrame
        Optional<WindowFrame> windowFrame = window.getWindowFrame();
        if (windowFrame.isPresent()) {
            WindowFrame wf = windowFrame.get();
            if (wf.getRightBoundary().is(FrameBoundType.UNBOUNDED_FOLLOWING)
                    && wf.getLeftBoundary().isNot(FrameBoundType.UNBOUNDED_PRECEDING)) {
                // reverse OrderKey's asc and isNullFirst;
                // in checkWindowFrameBeforeFunc(), we have confirmed that orderKeyLists must exist
                List<OrderKey> newOKList = window.getOrderKeyList().get().stream()
                        .map(orderKey -> new OrderKey(orderKey.getExpr(), !orderKey.isAsc(), !orderKey.isNullFirst()))
                        .collect(Collectors.toList());
                window = window.withOrderKeyList(newOKList);

                // reverse WindowFrame
                // e.g. (3 preceding, unbounded following) -> (unbounded preceding, 3 following)
                window = window.withWindowFrame(wf.reverseWindow());

                // reverse WindowFunction, which is used only for first_value() and last_value()
                Expression windowFunction = window.getWindowFunction();
                if (windowFunction instanceof FirstOrLastValue) {
                    window = window.withChildren(ImmutableList.of(((FirstOrLastValue) windowFunction).reverse()));
                }
            }
        } else {
            // this is equal to DEFAULT_WINDOW in class AnalyticWindow
            window = window.withWindowFrame(new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary()));
        }

    }

    /**
     *
     * if WindowFrame doesn't have right boundary, we will set it a default one(current row);
     * but if WindowFrame itself doesn't exist, we will keep it null still.
     *
     * Basic exception cases:
     * 0. WindowFrame != null, but OrderKeyList == null
     *
     * WindowFrame EXCEPTION cases:
     * 1. (unbounded following, xxx) || (offset following, !following)
     * 2. (xxx, unbounded preceding) || (!preceding, offset preceding)
     * 3. RANGE && ( (offset preceding, xxx) || (xxx, offset following) || (current row, current row) )
     *
     * WindowFrame boundOffset check:
     * 4. check value of boundOffset: Literal; Positive; Integer (for ROWS) or Numeric (for RANGE)
     * 5. check that boundOffset of left <= boundOffset of right
     */
    private void checkWindowFrame(WindowFrame windowFrame) {

        // case 0
        if (!window.getOrderKeyList().isPresent()) {
            throw new AnalysisException("WindowFrame clause requires OrderBy clause");
        }

        // set default rightBoundary
        if (windowFrame.getRightBoundary().isNull()) {
            windowFrame = windowFrame.withRightBoundary(FrameBoundary.newCurrentRowBoundary());
        }
        FrameBoundary left = windowFrame.getLeftBoundary();
        FrameBoundary right = windowFrame.getRightBoundary();

        // case 1
        if (left.getFrameBoundType() == FrameBoundType.UNBOUNDED_FOLLOWING) {
            throw new AnalysisException("WindowFrame in any window function cannot use "
                + "UNBOUNDED FOLLOWING as left boundary");
        }
        if (left.getFrameBoundType() == FrameBoundType.FOLLOWING && !right.asFollowing()) {
            throw new AnalysisException("WindowFrame with FOLLOWING left boundary requires "
                + "UNBOUNDED FOLLOWING or FOLLOWING right boundary");
        }

        // case 2
        if (right.getFrameBoundType() == FrameBoundType.UNBOUNDED_PRECEDING) {
            throw new AnalysisException("WindowFrame in any window function cannot use "
                + "UNBOUNDED PRECEDING as right boundary");
        }
        if (right.getFrameBoundType() == FrameBoundType.PRECEDING && !left.asPreceding()) {
            throw new AnalysisException("WindowFrame with PRECEDING right boundary requires "
                + "UNBOUNDED PRECEDING or PRECEDING left boundary");
        }

        // case 3
        // this case will be removed when RANGE with offset boundaries is supported
        if (windowFrame.getFrameUnits() == FrameUnitsType.RANGE) {
            if (left.hasOffset() || right.hasOffset()
                    || (left.getFrameBoundType() == FrameBoundType.CURRENT_ROW
                    && right.getFrameBoundType() == FrameBoundType.CURRENT_ROW)) {
                throw new AnalysisException("WindowFrame with RANGE must use both UNBOUNDED boundary or "
                    + "one UNBOUNDED boundary and one CURRENT ROW");
            }
        }

        // case 4
        if (left.hasOffset()) {
            checkFrameBoundOffset(left);
        }
        if (right.hasOffset()) {
            checkFrameBoundOffset(right);
        }

        // case 5
        // check correctness of left boundary and right boundary
        if (left.hasOffset() && right.hasOffset()) {
            double leftOffsetValue = ((Literal) left.getBoundOffset().get()).getDouble();
            double rightOffsetValue = ((Literal) right.getBoundOffset().get()).getDouble();
            if (left.asPreceding() && right.asPreceding()) {
                Preconditions.checkArgument(leftOffsetValue >= rightOffsetValue, "WindowFrame with "
                        + "PRECEDING boundary requires that leftBoundOffset >= rightBoundOffset");
            } else if (left.asFollowing() && right.asFollowing()) {
                Preconditions.checkArgument(leftOffsetValue <= rightOffsetValue, "WindowFrame with "
                        + "FOLLOWING boundary requires that leftBoundOffset >= rightBoundOffset");
            }
        }

        window = window.withWindowFrame(windowFrame);
    }

    /**
     * check boundOffset of FrameBoundary if it exists:
     * 1 boundOffset should be Literal, but this restriction can be removed after completing FoldConstant
     * 2 boundOffset should be positive
     * 2 boundOffset should be a positive INTEGER if FrameUnitsType == ROWS
     * 3 boundOffset should be a positive INTEGER or DECIMAL if FrameUnitsType == RANGE
     */
    private void checkFrameBoundOffset(FrameBoundary frameBoundary) {
        Expression offset = frameBoundary.getBoundOffset().get();

        // case 1
        Preconditions.checkArgument(offset.isLiteral(), "BoundOffset of WindowFrame must be Literal");

        // case 2
        boolean isPositive = ((Literal) offset).getDouble() > 0;
        Preconditions.checkArgument(isPositive, "BoundOffset of WindowFrame must be positive");

        FrameUnitsType frameUnits = window.getWindowFrame().get().getFrameUnits();
        // case 3
        if (frameUnits == FrameUnitsType.ROWS) {
            Preconditions.checkArgument(offset.getDataType().isIntegralType(),
                    "BoundOffset of ROWS WindowFrame must be an Integer");
        }

        // case 4
        if (frameUnits == FrameUnitsType.RANGE) {
            Preconditions.checkArgument(offset.getDataType().isNumericType(),
                    "BoundOffset of RANGE WindowFrame must be an Integer or Decimal");
        }
    }

    /**
     * required WindowFrame: (UNBOUNDED PRECEDING, offset PRECEDING)
     * but in Spark, it is (offset PRECEDING, offset PRECEDING)
     */
    @Override
    public Lag visitLag(Lag lag, CheckContext ctx) {
        // check and complete window frame
        window.getWindowFrame().ifPresent(wf -> {
            throw new AnalysisException("WindowFrame for LAG() must be null");
        });

        Expression column = lag.child(0);
        Expression offset = lag.getOffset();
        Expression defaultValue = lag.getDefaultValue();
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newPrecedingBoundary(offset));
        window = window.withWindowFrame(requiredFrame);

        // check if the class of lag's column matches defaultValue, and cast it
        if (!TypeCoercionUtils.implicitCast(column.getDataType(), defaultValue.getDataType()).isPresent()) {
            throw new AnalysisException("DefaultValue's Datatype of LAG() cannot match its relevant column. The column "
                + "type is " + column.getDataType() + ", but the defaultValue type is " + defaultValue.getDataType());
        }
        lag.setDefaultValue(TypeCoercionUtils.castIfNotSameType(defaultValue, column.getDataType()));

        // todo: should OrderKey be required?
        return lag;
    }

    /**
     * required WindowFrame: (UNBOUNDED PRECEDING, offset FOLLOWING)
     * but in Spark, it is (offset FOLLOWING, offset FOLLOWING)
     */
    @Override
    public Lead visitLead(Lead lead, CheckContext ctx) {
        window.getWindowFrame().ifPresent(wf -> {
            throw new AnalysisException("WindowFrame for LEAD() must be null");
        });

        Expression column = lead.child(0);
        Expression offset = lead.getOffset();
        Expression defaultValue = lead.getDefaultValue();
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newFollowingBoundary(offset));
        window = window.withWindowFrame(requiredFrame);

        // check if the class of lag's column matches defaultValue, and cast it
        if (!TypeCoercionUtils.implicitCast(column.getDataType(), defaultValue.getDataType()).isPresent()) {
            throw new AnalysisException("DefaultValue's Datatype of LEAD() can't match its relevant column. The column "
                + "type is " + column.getDataType() + ", but the defaultValue type is " + defaultValue.getDataType());
        }
        lead.setDefaultValue(TypeCoercionUtils.castIfNotSameType(defaultValue, column.getDataType()));

        // todo: should OrderKey be required?
        return lead;
    }

    /**
     * [Copied from class AnalyticExpr.standardize()]:
     *
     *    FIRST_VALUE without UNBOUNDED PRECEDING gets rewritten to use a different window
     *    and change the function to return the last value. We either set the fn to be
     *    'last_value' or 'first_value_rewrite', which simply wraps the 'last_value'
     *    implementation but allows us to handle the first rows in a partition in a special
     *    way in the backend. There are a few cases:
     *     a) Start bound is X FOLLOWING or CURRENT ROW (X=0):
     *        Use 'last_value' with a window where both bounds are X FOLLOWING (or
     *        CURRENT ROW). Setting the start bound to X following is necessary because the
     *        X rows at the end of a partition have no rows in their window. Note that X
     *        FOLLOWING could be rewritten as lead(X) but that would not work for CURRENT
     *        ROW.
     *     b) Start bound is X PRECEDING and end bound is CURRENT ROW or FOLLOWING:
     *        Use 'first_value_rewrite' and a window with an end bound X PRECEDING. An
     *        extra parameter '-1' is added to indicate to the backend that NULLs should
     *        not be added for the first X rows.
     *     c) Start bound is X PRECEDING and end bound is Y PRECEDING:
     *        Use 'first_value_rewrite' and a window with an end bound X PRECEDING. The
     *        first Y rows in a partition have empty windows and should be NULL. An extra
     *        parameter with the integer constant Y is added to indicate to the backend
     *        that NULLs should be added for the first Y rows.
     */
    @Override
    public FirstOrLastValue visitFirstValue(FirstValue firstValue, CheckContext ctx) {
        Optional<WindowFrame> windowFrame = window.getWindowFrame();
        if (windowFrame.isPresent()) {
            WindowFrame wf = windowFrame.get();
            if (wf.getLeftBoundary().isNot(FrameBoundType.UNBOUNDED_PRECEDING)
                    && wf.getLeftBoundary().isNot(FrameBoundType.PRECEDING)) {
                window = window.withWindowFrame(wf.withRightBoundary(wf.getLeftBoundary()));
                LastValue lastValue = new LastValue(firstValue.child());
                return lastValue;
            }

            if (wf.getLeftBoundary().is(FrameBoundType.UNBOUNDED_PRECEDING)
                    && wf.getRightBoundary().isNot(FrameBoundType.PRECEDING)) {
                window = window.withWindowFrame(wf.withRightBoundary(FrameBoundary.newCurrentRowBoundary()));
            }
        } else {
            window = window.withWindowFrame(new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary()));
        }
        return firstValue;
    }

    /**
     * required WindowFrame: (UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public Rank visitRank(Rank rank, CheckContext ctx) {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, rank.getName());
        return rank;
    }

    /**
     * required WindowFrame: (UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public DenseRank visitDenseRank(DenseRank denseRank, CheckContext ctx) {
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, denseRank.getName());
        return denseRank;
    }

    /**
     * required WindowFrame: (UNBOUNDED PRECEDING, CURRENT ROW)
     */
    @Override
    public RowNumber visitRowNumber(RowNumber rowNumber, CheckContext ctx) {
        // check and complete window frame
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        checkAndCompleteWindowFrame(requiredFrame, rowNumber.getName());

        // todo: should OrderKey be required?
        return rowNumber;
    }

    /**
     * check if the current WindowFrame equals with the required WindowFrame; if current WindowFrame is null,
     * the requiredFrame should be used as current WindowFrame.
     */
    private void checkAndCompleteWindowFrame(WindowFrame requiredFrame, String functionName) {
        window.getWindowFrame().ifPresent(wf -> {
            if (!wf.equals(requiredFrame)) {
                throw new AnalysisException("WindowFrame for " + functionName + "() must be null "
                    + "or match with " + requiredFrame);
            }
        });
        window = window.withWindowFrame(requiredFrame);
    }

    /** context for window function check specifically */
    public static class CheckContext {

    }
}
