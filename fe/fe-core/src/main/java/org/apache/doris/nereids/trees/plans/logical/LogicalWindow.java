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
import org.apache.doris.nereids.rules.rewrite.logical.ResolveWindowFunction.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
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

    private WindowFrameGroup windowFrameGroup;

    private boolean isNormalized;

    public LogicalWindow(List<NamedExpression> outputExpressions,
                         List<NamedExpression> windowExpressions, CHILD_TYPE child) {
        this(outputExpressions, windowExpressions, null, child);
    }

    // just for test
    public LogicalWindow(List<NamedExpression> windowExpressions, CHILD_TYPE child) {
        this(null, windowExpressions, null, child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions,
                         WindowFrameGroup windowFrameGroup, CHILD_TYPE child) {
        this(outputExpressions, null, windowFrameGroup, child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions, List<NamedExpression> windowExpressions, WindowFrameGroup windowFrameGroup, CHILD_TYPE child) {
        this(outputExpressions, windowExpressions, windowFrameGroup, false, Optional.empty(), Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions, List<NamedExpression> windowExpressions, WindowFrameGroup windowFrameGroup, boolean isNormalized, CHILD_TYPE child) {
        this(outputExpressions, windowExpressions, windowFrameGroup, isNormalized, Optional.empty(), Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> outputExpressions, List<NamedExpression> windowExpressions, WindowFrameGroup windowFrameGroup, boolean isNormalized,
                         Optional<GroupExpression> groupExpression,
                         Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_WINDOW, groupExpression, logicalProperties, child);
        this.outputExpressions = outputExpressions;
        this.windowExpressions = windowExpressions;
        this.windowFrameGroup = windowFrameGroup;
        this.isNormalized = isNormalized;
    }

    public boolean isNormalized() {
        return isNormalized;
    }

    public void setNormalized(boolean isNormalized) {
        this.isNormalized = isNormalized;
    }

    public WindowFrameGroup getWindowFrameGroup() {
        return windowFrameGroup;
    }

    public List<NamedExpression> getWindowExpressions() {
        return windowExpressions;
    }

    @Override
    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalWindow<>(outputExpressions, windowExpressions, windowFrameGroup, isNormalized, children.get(0));
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return outputExpressions;
    }

    @Override
    public String toString() {
        if (windowExpressions != null) {
            return Utils.toSqlString("LogicalWindow",
                "isNormalized", isNormalized,
                "outputExpressions", outputExpressions,
                "windowExpressions", windowExpressions
            );
        }
        return Utils.toSqlString("LogicalWindow",
            "isNormalized", isNormalized,
            "outputExpressions", outputExpressions,
            "windowFrameGroup", windowFrameGroup.getGroupList()
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalWindow(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalWindow<>(outputExpressions, windowExpressions, windowFrameGroup, isNormalized, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalWindow<>(outputExpressions, windowExpressions, windowFrameGroup, isNormalized, Optional.empty(), logicalProperties, child());
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExpressions.stream()
            .map(NamedExpression::toSlot)
            .collect(ImmutableList.toImmutableList());

//        List<Slot> outputList;
//        if (windowExpressions != null) {
//            outputList = windowExpressions.stream()
//                .map(NamedExpression::toSlot)
//                .collect(Collectors.toList());
//            outputList.addAll(child().getOutput());
//            return outputList;
//        }
//
//        outputList = windowFrameGroup.getGroupList().stream()
//            .map(NamedExpression::toSlot)
//            .collect(Collectors.toList());
//        outputList.addAll(child().getOutput());
//        return outputList;
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
            && Objects.equals(windowFrameGroup, that.windowFrameGroup)
            && isNormalized == that.isNormalized;
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputExpressions, windowExpressions, windowFrameGroup, isNormalized);
    }
}
