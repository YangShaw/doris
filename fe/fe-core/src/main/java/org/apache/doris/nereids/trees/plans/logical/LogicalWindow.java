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
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import java.util.List;
import java.util.Optional;

/**
 * logical node to deal with window functions
 */
public class LogicalWindow<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {

    List<Window> windowExpressions;
    List<Expression> partitionSpec;
    List<OrderKey> orderSpec;


    public LogicalWindow(List<Window> windowExpressions, List<Expression> partitionSpec, List<OrderKey> orderSpec, CHILD_TYPE child) {
        super(PlanType.LOGICAL_WINDOW, child);
        this.windowExpressions = windowExpressions;
        this.partitionSpec = partitionSpec;
        this.orderSpec = orderSpec;
    }

    public LogicalWindow(Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_WINDOW, groupExpression, logicalProperties, child);
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
        return null;
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return null;
    }

    @Override
    public List<Slot> computeOutput() {
        return null;
    }
}
