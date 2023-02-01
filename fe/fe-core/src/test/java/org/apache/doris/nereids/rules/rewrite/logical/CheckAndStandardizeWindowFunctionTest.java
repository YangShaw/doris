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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.logical.RelationUtil;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CheckAndStandardizeWindowFunctionTest implements PatternMatchSupported {

    private LogicalPlan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(RelationUtil.newRelationId(), PlanConstructor.student, ImmutableList.of());
    }

    @Test
    public void test() {
        NamedExpression gender = rStudent.getOutput().get(1).toSlot();
        NamedExpression age = rStudent.getOutput().get(3).toSlot();

        List<Expression> partitionKeyList = ImmutableList.of(gender);
        List<OrderKey> orderKeyList = ImmutableList.of(new OrderKey(age, true, true));
        Window window = new Window(new Rank(), Optional.of(partitionKeyList), Optional.of(orderKeyList), Optional.empty());
        Alias windowAlias = new Alias(window, window.toSql());
        WindowFrame requiredFrame = new WindowFrame(FrameUnitsType.RANGE,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        List<NamedExpression> outputExpressions = Lists.newArrayList(windowAlias);
        Plan root = new LogicalWindow<>(outputExpressions, rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new NormalizeWindow())
                .applyTopDown(new CheckAndStandardizeWindowFunctionAndFrame())
                .matches(
                        logicalWindow()
                                .when(logicalWindow -> {
                                    Window newWindow = (Window) logicalWindow.getOutputExpressions().get(0).child(0);
                                    return newWindow.getWindowFrame().get().equals(requiredFrame);
                                })
                );
    }

}
