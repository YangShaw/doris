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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * NormalizeWindow: generate bottomProject for expressions within Window, and topProject for origin output of SQL
 * e.g. for SQL: SELECT k1#1, k2#2, SUM(k3#3) OVER (PARTITION BY k4#4 ORDER BY k5#5) FROM t
 *
 * Original Plan:
 * LogicalWindow(
 *   outputs:[k1#1, k2#2, Alias(SUM(k3#3) OVER (PARTITION BY k4#4 ORDER BY k5#5)#6],
 *   windowExpressions:[]
 *   )
 *
 * After Normalize:
 * Project(k1#1, k2#2, Alias(SlotReference#7)#7)
 * +-- LogicalWindow(
 *       outputs:[k1#1, k2#2, Alias(SUM(k3#3) OVER (PARTITION BY k4#4 ORDER BY k5#5)#6],
 *       windowExpressions:[Alias(SUM(k3#3) OVER (PARTITION BY k4#4 ORDER BY k5#5)#6]
 *       )
 *   +-- Project(k1#1, k2#2, k3#3, k4#4, k5#5)
 *
 */
public class NormalizeWindow extends OneRewriteRuleFactory implements NormalizeToSlot {

    @Override
    public Rule build() {
        return logicalWindow().whenNot(LogicalWindow::isNormalized).then(logicalWindow -> {

            List<NamedExpression> outputs = logicalWindow.getOutputExpressions();

            // 1. handle bottom projects
            Set<Alias> existedAlias = ExpressionUtils.collect(outputs, Alias.class::isInstance);
            Set<Expression> toBePushedDown = collectExpressionsToBePushedDown(logicalWindow);
            NormalizeToSlotContext context =
                    NormalizeToSlotContext.buildContext(existedAlias, toBePushedDown);
            // set toBePushedDown exprs as NamedExpression, e.g. (a+1) -> Alias(a+1)
            Set<NamedExpression> bottomProjects = context.pushDownToNamedExpression(toBePushedDown);
            Plan normalizedChild = bottomProjects.isEmpty()
                    ? logicalWindow.child()
                    : new LogicalProject<>(ImmutableList.copyOf(bottomProjects), logicalWindow.child());

            // 2. handle window's outputs and windowExprs
            List<NamedExpression> normalizedOutputs = context.normalizeToUseSlotRef(outputs);
            Set<Window> normalizedWindows = ExpressionUtils.collect(normalizedOutputs, Window.class::isInstance);

            existedAlias = ExpressionUtils.collect(normalizedOutputs, Alias.class::isInstance);
            NormalizeToSlotContext ctxForWindows = NormalizeToSlotContext.buildContext(existedAlias, normalizedWindows);

            Set<NamedExpression> normalizedWindowsWithAlias =
                    ctxForWindows.pushDownToNamedExpression(normalizedWindows);
            Set<Slot> normalizedOthers = ExpressionUtils.collect(normalizedOutputs, SlotReference.class::isInstance);
            List<NamedExpression> normalizedWindowOutputs = ImmutableList.<NamedExpression>builder()
                    .addAll(normalizedOthers)
                    .addAll(normalizedWindowsWithAlias)
                    .build();
            LogicalWindow normalizedLogicalWindow =
                    logicalWindow.withNormalized(normalizedWindowOutputs,
                            ImmutableList.copyOf(normalizedWindowsWithAlias), normalizedChild);

            // 3. generate top projects
            List<NamedExpression> topProjects = ctxForWindows.normalizeToUseSlotRef(normalizedOutputs);
            return new LogicalProject<>(topProjects, normalizedLogicalWindow);
        }).toRule(RuleType.NORMALIZE_WINDOW);
    }

    private Set<Expression> collectExpressionsToBePushedDown(LogicalWindow<? extends Plan> logicalWindow) {
        // bottomProjects includes:
        // 1. expressions from WindowSpec's partitionKeys and orderKeys
        // 2. expressions from WindowFunctions arguments
        // 3. other slots of outputExpressions

        ImmutableSet.Builder<Expression> builder = new ImmutableSet.Builder<>();

        Set<Window> windows = ExpressionUtils.collect(logicalWindow.getWindowExpressions(), Window.class::isInstance);
        ImmutableSet<Expression> exprsInWindowSpec = windows.stream()
                .flatMap(window -> window.getExpressionsInWindowSpec().stream())
                .collect(ImmutableSet.toImmutableSet());

        //        ImmutableSet<Expression> argsOfFunctionInWindow = windows.stream()
        //                .flatMap(window -> window.getWindowFunction().getArguments().stream())
        //                .collect(ImmutableSet.toImmutableSet());

        ImmutableSet<Expression> otherSlots = logicalWindow.getOutputExpressions().stream()
                .filter(expr -> !expr.anyMatch(Window.class::isInstance))
                .collect(ImmutableSet.toImmutableSet());

        return builder.addAll(exprsInWindowSpec)
            .addAll(otherSlots)
            .build();
    }
}
