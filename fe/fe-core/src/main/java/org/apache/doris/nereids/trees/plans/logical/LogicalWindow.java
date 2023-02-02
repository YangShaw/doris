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

/**
 * logical node to deal with window functions
 */
public class LogicalWindow<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Window {

    private final List<NamedExpression> outputExpressions;

    private final boolean isChecked;

    /**
     * used in step Analyze
     */
    public LogicalWindow(List<NamedExpression> outputExpressions, CHILD_TYPE child) {
        this(outputExpressions, false, Optional.empty(), Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions, boolean isChecked, CHILD_TYPE child) {
        this(outputExpressions, isChecked, Optional.empty(), Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions, boolean isChecked,
                         Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
                         CHILD_TYPE child) {
        super(PlanType.LOGICAL_WINDOW, groupExpression, logicalProperties, child);
        this.outputExpressions = ImmutableList.copyOf(Objects.requireNonNull(outputExpressions, "output expressions" +
                "in LogicalWindow cannot be null"));
        this.isChecked = isChecked;
    }

    public boolean isChecked() {
        return isChecked;
    }

    @Override
    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    /**
     * get all expressions used in LogicalWindow
     */
    @Override
    public List<? extends Expression> getExpressions() {
        return outputExpressions;
    }

    public LogicalWindow withChecked(List<NamedExpression> outputExpressions, Plan child) {
        return new LogicalWindow(outputExpressions, true, Optional.empty(), Optional.empty(), child);
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalWindow<>(outputExpressions, isChecked, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalWindow(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalWindow<>(outputExpressions, isChecked,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalWindow<>(outputExpressions, isChecked,
                Optional.empty(), logicalProperties, child());
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExpressions.stream()
            .map(NamedExpression::toSlot)
            .collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalWindow",
            "isChecked", isChecked,
            "outputExpressions", outputExpressions
        );
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
            && isChecked == that.isChecked;
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputExpressions, isChecked);
    }
}
