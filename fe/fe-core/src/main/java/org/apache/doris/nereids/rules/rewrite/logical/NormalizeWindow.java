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
 * NormalizeWindow
 * todo: add examples
 */
public class NormalizeWindow extends OneRewriteRuleFactory implements NormalizeToSlot {

    @Override
    public Rule build() {
        return logicalWindow().whenNot(LogicalWindow::isNormalized).then(logicalWindow -> {

            List<NamedExpression> outputs = logicalWindow.getOutputExpressions();

            Set<Alias> existedAlias = ExpressionUtils.collect(outputs, Alias.class::isInstance);
            Set<Expression> toBePushedDown = collectExpressionsToBePushedDown(logicalWindow);
            NormalizeToSlotContext context =
                    NormalizeToSlotContext.buildContext(existedAlias, toBePushedDown);
            Set<NamedExpression> bottomProjects = context.pushDownToNamedExpression(toBePushedDown);
            Plan normalizedChild = bottomProjects.isEmpty()
                    ? logicalWindow.child()
                    : new LogicalProject<>(ImmutableList.copyOf(bottomProjects), logicalWindow.child());

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

            List<NamedExpression> topProjects = ctxForWindows.normalizeToUseSlotRef(normalizedOutputs);
            return new LogicalProject<>(topProjects, normalizedLogicalWindow);

//            Map<Expression, Expression> substitutionMap = Maps.newHashMap();
//            // projects of bottom LogicalProject
//            List<NamedExpression> bottomProjections = Lists.newArrayList();
//            // outputs of LogicalWindow
//            List<NamedExpression> newOutputs = Lists.newArrayList();
//
//            Map<Boolean, List<Expression>> partitionedOutputs = outputs.stream()
//                    .collect(Collectors.groupingBy(SlotReference.class::isInstance));
//
//            if (partitionedOutputs.containsKey(true)) {
//                // other slots
//                // add to botProj、newOutputs
//                partitionedOutputs.get(true).stream()
//                    .map(SlotReference.class::cast)
//                    .peek(s -> substitutionMap.put(s, s))
//                    .peek(bottomProjections::add)
//                    .forEach(newOutputs::add);
//            }
//
//            if (partitionedOutputs.containsKey(false)) {
//                // slots of WindowAlias
//                for (Expression expression : partitionedOutputs.get(false)) {
//                    Window window = (Window) expression.child(0);
//
//                    // expr in window specs (partition by and order by clause)
//                    List<NamedExpression> exprInWindowSpecs = window.getExpressions().stream()
//                            .map(SlotReference.class::cast)
//                            .collect(Collectors.toList());
//                    bottomProjections.addAll(exprInWindowSpecs);
//
//                    // expr in function's arguments
//                    WindowFunction function = (WindowFunction) window.getWindowFunction();
//                    List<Expression> newChildren = Lists.newArrayList();
//                    for (Expression child : function.getArguments()) {
//                        if (child instanceof SlotReference || child instanceof Literal) {
//                            newChildren.add(child);
//                            if (child instanceof SlotReference) {
//                                bottomProjections.add((SlotReference) child);
//                            }
//                        } else {
//                            Alias alias = new Alias(child, child.toSql());
//                            bottomProjections.add(alias);
//                            newChildren.add(alias.toSlot());
//                        }
//                    }
//                    WindowFunction newFunction = (WindowFunction) function.withChildren(newChildren);
//                    Window newWindow = window.withChildren(ImmutableList.of(newFunction));
//                    Alias alias = new Alias(newWindow, newWindow.toSql());
//                    newOutputs.add(alias);
//                    windowExpressions.add(alias);
//                    substitutionMap.put(window, alias.toSlot());
//                }
//            }
//            LogicalProject bottomProject = new LogicalProject(bottomProjections, logicalWindow.child());
//            LogicalWindow newWindow = new LogicalWindow(newOutputs, windowExpressions, bottomProject);
//            newWindow.setNormalized(true);
//            // projects of top LogicalProject
//            List<NamedExpression> topProjections = outputs.stream()
//                    .map(e -> ExpressionUtils.replace(e, substitutionMap))
//                    .map(NamedExpression.class::cast)
//                    .collect(Collectors.toList());
//            LogicalProject topProject = new LogicalProject(topProjections, newWindow);
//            return topProject;
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
                .flatMap(window -> window.getExpressions().stream())
                .collect(ImmutableSet.toImmutableSet());

        ImmutableSet<Expression> argsOfFunctionInWindow = windows.stream()
                .flatMap(window -> window.getWindowFunction().getArguments().stream())
                .collect(ImmutableSet.toImmutableSet());

        ImmutableSet<Expression> otherSlots = logicalWindow.getOutputExpressions().stream()
                .filter(expr -> !expr.anyMatch(Window.class::isInstance))
                .collect(ImmutableSet.toImmutableSet());

        return builder.addAll(exprsInWindowSpec)
            .addAll(argsOfFunctionInWindow)
            .addAll(otherSlots)
            .build();
    }
}
