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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.logical.ResolveWindowFunction.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.StatsDeriveResult;

import java.util.List;
import java.util.Optional;

/**
 * physical node for window function
 */
public class PhysicalWindow<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Window {

    private List<NamedExpression> outputExpressions;
    private List<NamedExpression> windowExpressions;
    private List<Expression> partitionSpec;
    private List<OrderKey> orderSpec;
    private WindowFrameGroup windowFrameGroup;

    public PhysicalWindow(List<NamedExpression> outputExpressions, WindowFrameGroup windowFrameGroup, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(outputExpressions, windowFrameGroup, Optional.empty(), logicalProperties, child);
    }

    public PhysicalWindow(List<NamedExpression> outputExpressions, WindowFrameGroup windowFrameGroup, Optional<GroupExpression> groupExpression,
                          LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_WINDOW, groupExpression, logicalProperties, child);
        this.outputExpressions = outputExpressions;
        this.windowFrameGroup = windowFrameGroup;
    }

    public PhysicalWindow(List<NamedExpression> outputExpressions, WindowFrameGroup windowFrameGroup,
                          Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                          PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult,
                          CHILD_TYPE child) {
        super(PlanType.PHYSICAL_WINDOW, groupExpression, logicalProperties, physicalProperties, statsDeriveResult,
            child);
        this.outputExpressions = outputExpressions;
        this.windowFrameGroup = windowFrameGroup;
    }

    @Override
    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public List<NamedExpression> getWindowExpressions() {
        return windowExpressions;
    }

    public List<Expression> getPartitionSpec() {
        return partitionSpec;
    }

    public List<OrderKey> getOrderSpec() {
        return orderSpec;
    }

    public WindowFrameGroup getWindowFrameGroup() {
        return windowFrameGroup;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return null;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return null;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return null;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalWindow<>(outputExpressions, windowFrameGroup, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalWindow<>(outputExpressions, windowFrameGroup, Optional.empty(), logicalProperties.get(), child());
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                       StatsDeriveResult statsDeriveResult) {
        return new PhysicalWindow<>(outputExpressions, windowFrameGroup, Optional.empty(), getLogicalProperties(),
            physicalProperties, statsDeriveResult, child());
    }
}
