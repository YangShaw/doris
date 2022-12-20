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

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AnalysisRuleFactory;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * extract window expressions from LogicalProject, LogicalSort, LogicalAggregate
 */
public class ExtractWindowExpression implements AnalysisRuleFactory {

    /**
     * Matched patterns:
     * 1 LogicalSort -> LogicalAggregate
     * 2 LogicalSort -> LogicalProject
     * 3 LogicalProject
     * 4 LogicalAggregate
     * 5 LogicalSort
     */
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            /*RuleType.WINDOW_FUNCTION_FROM_SORT_AGG.build(
                logicalSort(logicalAggregate()).thenApply(ctx -> {
                    LogicalSort<LogicalAggregate<GroupPlan>> logicalSort = ctx.root;

                    List<Expression> expressionList = logicalSort.getOrderKeys().stream()
                        .map(orderKey -> orderKey.getExpr()).collect(Collectors.toList());
                    List<NamedExpression> windowList = extractWindowExpression(expressionList);

                    LogicalAggregate<GroupPlan> logicalAggregate = ctx.root.child();
                    List<NamedExpression> outputExpressions = logicalAggregate.getOutputExpressions();
                    windowList.addAll(extractWindowExpression(outputExpressions));

                    if (windowList.isEmpty()) {
                        return logicalSort;
                    }
                    return new LogicalWindow(windowList, newLogicalProject);
                })
            ),
            RuleType.WINDOW_FUNCTION_FROM_SORT_PROJECT.build(
                logicalSort(logicalProject()).thenApply(ctx -> {
                    LogicalSort<LogicalProject<GroupPlan>> logicalSort = ctx.root;

                    List<Expression> expressionList = logicalSort.getOrderKeys().stream()
                        .map(orderKey -> orderKey.getExpr()).collect(Collectors.toList());
                    List<NamedExpression> windowList = extractWindowExpression(expressionList);

                    LogicalProject<GroupPlan> logicalProject = ctx.root.child();
                    List<NamedExpression> projects = logicalProject.getProjects();
                    windowList.addAll(extractWindowExpression(projects));

                    if (windowList.isEmpty()) {
                        return logicalProject;
                    }
                    return new LogicalWindow(windowList, newLogicalProject);
                })
            ),*/
            RuleType.WINDOW_FUNCTION_FROM_PROJECT.build(
                logicalProject().whenNot(LogicalProject::hasExtractedWindow)
                    .then(logicalProject -> extractDuringAnalyze(logicalProject, logicalProject.getProjects()))
            ),
            RuleType.WINDOW_FUNCTION_FROM_AGG.build(
                logicalAggregate().when(aggregate -> containsWindowExpression(aggregate.getOutputExpressions()))
                    .then(logicalAggregate ->
                        extractWindowExpression(logicalAggregate, logicalAggregate.getOutputExpressions()))
            ),
            RuleType.WINDOW_FUNCTION_FROM_SORT.build(
                logicalSort().when(sort -> containsWindowExpression(sort.getOrderKeys().stream()
                        .map(orderKey -> orderKey.getExpr()).collect(Collectors.toList())))
                    .then(logicalSort -> extractWindowExpression(logicalSort, logicalSort.getOrderKeys().stream()
                        .map(orderKey -> orderKey.getExpr()).collect(Collectors.toList())))
            )
        );
    }

    private <E extends Expression> LogicalPlan extractDuringAnalyze(LogicalPlan root, List<E> expressionList) {
        List<Expression> unboundWindowList = Lists.newArrayList();
        List<Expression> remainedExpressionList = Lists.newArrayList();

        expressionList.forEach(expression -> {
            if (expression instanceof UnboundAlias && expression.child(0) instanceof Window) {
                unboundWindowList.add(expression);
            } else {
                remainedExpressionList.add(expression);
            }
        });

        if (unboundWindowList.isEmpty()) {
            return new LogicalProject(expressionList, true, root.child(0));
        }

        unboundWindowList.forEach(unboundWindow -> {
            remainedExpressionList.addAll(((Window)(unboundWindow.child(0))).getExpression());
        });

        LogicalProject newLogicalProject = new LogicalProject(remainedExpressionList, true, root.child(0));
        LogicalWindow logicalWindow = new LogicalWindow(unboundWindowList, newLogicalProject);

        return logicalWindow;
//        return new LogicalProject(expressionList, true, logicalWindow);
    }

    private <E extends Expression> LogicalPlan extractWindowExpression(LogicalPlan root, List<E> expressionList) {
        List<NamedExpression> windowList = Lists.newArrayList();
        List<E> remainedExpressionList = Lists.newArrayList();
        extractWindowExpression(windowList, remainedExpressionList, expressionList);

        LogicalProject newLogicalProject = new LogicalProject(remainedExpressionList, root.child(0));

        if (windowList.isEmpty()) {
            return newLogicalProject;
        }
        return new LogicalWindow(windowList, newLogicalProject);
    }

    private <E extends Expression> void extractWindowExpression(
            List<NamedExpression> windowList, List<E> remainedExpressionList, List<E> expressionList) {

        expressionList.forEach(expression -> {
            if (expression instanceof Alias && expression.child(0) instanceof Window) {
                windowList.add((Alias) expression);
            } else {
                remainedExpressionList.add(expression);
            }
        });
    }

    private <E extends Expression> boolean containsWindowExpression(List<E> expressionList) {
//        return expressionList.stream().anyMatch(expression ->
//            expression instanceof Alias && expression.child(0) instanceof Window
//        );

        return expressionList.stream().anyMatch(expression ->
            expression instanceof UnboundAlias && expression.child(0) instanceof Window
        );
    }

}
