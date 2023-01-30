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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * NormalizeWindow: generate bottomProject for expressions within Window, and topProject for origin output of SQL
 * e.g. SELECT k1#1, k2#2, SUM(k3#3) OVER (PARTITION BY k4#4 ORDER BY k5#5) FROM t
 *
 * Original Plan:
 * LogicalWindow(
 *   outputs:[k1#1, k2#2, Alias(SUM(k3#3) OVER (PARTITION BY k4#4 ORDER BY k5#5)#6],
 *   windowExpressions:[]
 *   )
 *
 * After Normalize:
 * LogicalProject(k1#1, k2#2, Alias(SlotReference#7)#6)
 * +-- LogicalWindow(
 *       outputs:[k1#1, k2#2, Alias(SUM(k3#3) OVER (PARTITION BY k4#4 ORDER BY k5#5)#6],
 *       windowExpressions:[Alias(SUM(k3#3) OVER (PARTITION BY k4#4 ORDER BY k5#5)#6]
 *       )
 *   +-- LogicalProject(k1#1, k2#2, k3#3, k4#4, k5#5)
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
            // need to replace exprs with SlotReference in WindowSpec, due to LogicalWindow.getExpressions()
            List<NamedExpression> normalizedOutputs = context.normalizeToUseSlotRef(outputs);
            List<Window> normalizedWindows = normalizeToUseSlotRefInWindow(
                    ExpressionUtils.collect(normalizedOutputs, Window.class::isInstance), context);
            Set<Slot> normalizedOthers = ExpressionUtils.collect(normalizedOutputs, SlotReference.class::isInstance);

            existedAlias = ExpressionUtils.collect(normalizedOutputs, Alias.class::isInstance);
            NormalizeToSlotContext ctxForWindows = NormalizeToSlotContext.buildContext(
                    existedAlias, Sets.newHashSet(normalizedWindows));

            Set<NamedExpression> normalizedWindowWithAlias = ctxForWindows.pushDownToNamedExpression(normalizedWindows);
            List<NamedExpression> outputsWithNormalizedWindow = ImmutableList.<NamedExpression>builder()
                    .addAll(normalizedOthers)
                    .addAll(normalizedWindowWithAlias)
                    .build();
            LogicalWindow normalizedLogicalWindow = logicalWindow.withNormalized(
                    outputsWithNormalizedWindow, Lists.newArrayList(normalizedWindowWithAlias), normalizedChild);

            // 3. handle top projects
            existedAlias = ExpressionUtils.collect(outputsWithNormalizedWindow, Alias.class::isInstance);
            NormalizeToSlotContext ctxForTopProject = NormalizeToSlotContext.buildContext(
                    existedAlias, Sets.newHashSet(outputsWithNormalizedWindow)
            );
            List<NamedExpression> topProjects = ctxForTopProject.normalizeToUseSlotRef(outputsWithNormalizedWindow);
            return new LogicalProject<>(topProjects, normalizedLogicalWindow);
        }).toRule(RuleType.NORMALIZE_WINDOW);
    }

    private Set<Expression> collectExpressionsToBePushedDown(LogicalWindow<? extends Plan> logicalWindow) {
        // bottomProjects includes:
        // 1. expressions from function and WindowSpec's partitionKeys and orderKeys
        // 2. other slots of outputExpressions

        ImmutableSet.Builder<Expression> builder = new ImmutableSet.Builder<>();

        Set<Window> windows = ExpressionUtils.collect(logicalWindow.getOutputExpressions(), Window.class::isInstance);
        ImmutableSet<Expression> exprsInWindowSpec = windows.stream()
                .flatMap(window -> window.getExpressionsInWindowSpec().stream())
                .collect(ImmutableSet.toImmutableSet());

        ImmutableSet<Expression> otherSlots = logicalWindow.getOutputExpressions().stream()
                .filter(expr -> !expr.anyMatch(Window.class::isInstance))
                .collect(ImmutableSet.toImmutableSet());

        return builder.addAll(exprsInWindowSpec)
            .addAll(otherSlots)
            .build();
    }

    private List<Window> normalizeToUseSlotRefInWindow(Set<Window> windows, NormalizeToSlotContext context) {
        List<Window> normalizedWindows = Lists.newArrayList();
        for (Window window : windows) {
            Expression newFunction = context.normalizeToUseSlotRef(window.children()).get(0);

            Optional<List<Expression>> newPartitionKeyList;
            if (window.getPartitionKeyList().isPresent()) {
                newPartitionKeyList = Optional.of(context.normalizeToUseSlotRef(window.getPartitionKeyList().get()));
            } else {
                newPartitionKeyList = Optional.empty();
            }

            // todo: trick; use OrderExpression to save window's OrderKeyList
            Optional<List<OrderKey>> newOrderKeyList;
            if (window.getOrderKeyList().isPresent()) {
                newOrderKeyList = Optional.of(
                    window.getOrderKeyList().get().stream().map(orderKey -> {
                        Expression newExpr = context.normalizeToUseSlotRef(orderKey.getExpr());
                        if (newExpr != null && newExpr == orderKey.getExpr()) {
                            return orderKey;
                        }
                        return new OrderKey(newExpr, orderKey.isAsc(), orderKey.isNullFirst());
                    }).collect(Collectors.toList())
                );
            } else {
                newOrderKeyList = Optional.empty();
            }
            normalizedWindows.add(new Window(
                    newFunction, newPartitionKeyList, newOrderKeyList, window.getWindowFrame()));
        }
        return normalizedWindows;
    }
}
