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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NormalizeWindow extends OneRewriteRuleFactory {

    public Rule build() {
        return logicalWindow().whenNot(LogicalWindow::isNormalized).then(logicalWindow -> {

            List<NamedExpression> outputs = logicalWindow.getOutputExpressions();
            List<NamedExpression> windowExpressions = Lists.newArrayList();
            Map<Expression, Expression> substitutionMap = Maps.newHashMap();
            // projects of bottom LogicalProject
            List<NamedExpression> bottomProjections = Lists.newArrayList();
            // outputs of LogicalWindow
            List<NamedExpression> newOutputs = Lists.newArrayList();

            Map<Boolean, List<Expression>> partitionedOutputs = outputs.stream()
                .collect(Collectors.groupingBy(SlotReference.class::isInstance));

            if (partitionedOutputs.containsKey(true)) {
                // other slots
                // add to botProj、newOutputs
                partitionedOutputs.get(true).stream()
                    .map(SlotReference.class::cast)
                    .peek(s -> substitutionMap.put(s, s))
                    .peek(bottomProjections::add)
                    .forEach(newOutputs::add);
            }

            if (partitionedOutputs.containsKey(false)) {
                // slots of WindowAlias
                for (Expression expression : partitionedOutputs.get(false)) {
                    Window window = (Window) expression.child(0);

                    // expr in window specs (partition by and order by clause)
                    List<NamedExpression> exprInWindowSpecs = window.getExpression().stream()
                        .map(SlotReference.class::cast)
                        .collect(Collectors.toList());
                    bottomProjections.addAll(exprInWindowSpecs);

                    // expr in function's arguments
                    WindowFunction function = (WindowFunction) window.getWindowFunction();
                    List<Expression> newChildren = Lists.newArrayList();
                    for (Expression child : function.getArguments()) {
                        if (child instanceof SlotReference || child instanceof Literal) {
                            newChildren.add(child);
                            if (child instanceof SlotReference) {
                                bottomProjections.add((SlotReference) child);
                            }
                        } else {
                            Alias alias = new Alias(child, child.toSql());
                            bottomProjections.add(alias);
                            newChildren.add(alias.toSlot());
                        }
                    }
                    WindowFunction newFunction = (WindowFunction) function.withChildren(newChildren);
                    Window newWindow = window.withChildren(ImmutableList.of(newFunction));
                    Alias alias = new Alias(newWindow, newWindow.toSql());
                    newOutputs.add(alias);
                    windowExpressions.add(alias);
                    substitutionMap.put(window, alias.toSlot());
                }
            }
            LogicalProject bottomProject = new LogicalProject(bottomProjections, logicalWindow.child());
            LogicalWindow newWindow = new LogicalWindow(newOutputs, windowExpressions, bottomProject);
            newWindow.setNormalized(true);
            // projects of top LogicalProject
            List<NamedExpression> topProjections = outputs.stream()
                .map(e -> ExpressionUtils.replace(e, substitutionMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
            LogicalProject topProject = new LogicalProject(topProjections, newWindow);
            return topProject;
        }).toRule(RuleType.NORMALIZE_WINDOW);
    }
}
