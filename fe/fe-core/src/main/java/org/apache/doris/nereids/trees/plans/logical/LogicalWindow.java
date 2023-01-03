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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * logical node to deal with window functions
 */
public class LogicalWindow<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Window {

    private List<NamedExpression> outputExpressions;

    private List<NamedExpression> windowExpressions;

    private boolean isNormalized;

    private boolean isChecked;

    public LogicalWindow(List<NamedExpression> outputExpressions, CHILD_TYPE child) {
        this(outputExpressions, null, child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions, List<NamedExpression> windowExpressions,
                         CHILD_TYPE child) {
        this(outputExpressions, windowExpressions, false, false, Optional.empty(),
                Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions, List<NamedExpression> windowExpressions,
                         boolean isNormalized, boolean isChecked, CHILD_TYPE child) {
        this(outputExpressions, windowExpressions, isNormalized, isChecked, Optional.empty(),
                Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions, List<NamedExpression> windowExpressions,
                         boolean isNormalized, boolean isChecked,
                         Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
                         CHILD_TYPE child) {
        super(PlanType.LOGICAL_WINDOW, groupExpression, logicalProperties, child);
        this.outputExpressions = outputExpressions;
        this.windowExpressions = windowExpressions;
        this.isNormalized = isNormalized;
        this.isChecked = isChecked;
    }

    public boolean isNormalized() {
        return isNormalized;
    }

    public boolean isChecked() {
        return isChecked;
    }

    public void setNormalized(boolean isNormalized) {
        this.isNormalized = isNormalized;
    }

    public List<NamedExpression> getWindowExpressions() {
        return windowExpressions;
    }

    public LogicalWindow withWindowExpressions(List<NamedExpression> windowExpressions) {
        return new LogicalWindow<>(outputExpressions, windowExpressions, isNormalized, isChecked, child());
    }

    public LogicalWindow withNormalized(List<NamedExpression> outputExpressions,
                                        List<NamedExpression> windowExpressions, Plan normalizedChild) {
        return new LogicalWindow(outputExpressions, windowExpressions, true, isChecked,
            Optional.empty(), Optional.empty(), normalizedChild);
    }

    public LogicalWindow withChecked(List<NamedExpression> outputExpressions,
                                        List<NamedExpression> windowExpressions, Plan child) {
        return new LogicalWindow(outputExpressions, windowExpressions, isNormalized, true,
            Optional.empty(), Optional.empty(), child);
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalWindow<>(outputExpressions, windowExpressions,
            isNormalized, isChecked, children.get(0));
    }

    @Override
    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        ImmutableList.Builder<Expression> builder = new ImmutableList.Builder<>();
        builder.addAll(outputExpressions);
        if (windowExpressions != null) {
            builder.addAll(extractExpressionsFromWindow(windowExpressions));
        } else {
            builder.addAll(extractExpressionsFromWindow(outputExpressions.stream()
                    .filter(expr -> expr.anyMatch(org.apache.doris.nereids.trees.expressions.Window.class::isInstance))
                    .collect(Collectors.toList())));
        }
        return builder.build();
    }

    private List<Expression> extractExpressionsFromWindow(List<NamedExpression> windowExpressions) {
        return windowExpressions.stream().map(expression -> expression.child(0))
            .map(org.apache.doris.nereids.trees.expressions.Window.class::cast)
            .flatMap(window -> window.getExpressions().stream())
            .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        if (windowExpressions != null) {
            return Utils.toSqlString("LogicalWindow",
                "isNormalized", isNormalized,
                "isChecked", isChecked,
                "outputExpressions", outputExpressions,
                "windowExpressions", windowExpressions
            );
        }
        return Utils.toSqlString("LogicalWindow",
            "isNormalized", isNormalized,
            "isChecked", isChecked,
            "outputExpressions", outputExpressions
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalWindow(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalWindow<>(outputExpressions, windowExpressions, isNormalized, isChecked,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalWindow<>(outputExpressions, windowExpressions, isNormalized, isChecked,
                Optional.empty(), logicalProperties, child());
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExpressions.stream()
            .map(NamedExpression::toSlot)
            .collect(ImmutableList.toImmutableList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalWindow<?> that = (LogicalWindow<?>) o;
        return Objects.equals(outputExpressions, that.outputExpressions)
            && Objects.equals(windowExpressions, that.windowExpressions)
            && isNormalized == that.isNormalized
            && isChecked == that.isChecked;
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputExpressions, windowExpressions, isNormalized, isChecked);
    }
}
