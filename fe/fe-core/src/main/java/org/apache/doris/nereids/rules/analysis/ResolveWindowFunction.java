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

package org.apache.doris.nereids.rules.analysis;

import com.google.common.collect.ImmutableList;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameUnitsType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 目标：创建逻辑节点，来维护窗口函数相关的信息。窗口函数对排序有要求，因此也需要增加Sort相关的算子；
 * 为了避免重复排序（分区字段也相当于排序），需要对不同窗口函数的信息进行分析，合并同类项。
 *
 * 标准化部分
 * 1 对每一个Window做检查：根据函数类型检查order、frame；检查函数类型是否支持；
 * 2 补充窗口，进行标准化，不同的窗口函数有不同的标准化规则（不确定要在几步中进行）
 *
 * 合并同类项部分
 * *1 计算三种Group:
 *      WindowFrameGroup:分区、排序、窗口都相同
 *      OrderKeyGroup: 分区、排序相同
 *      PartitionKeyGroup: 分区相同
 * *2 在PartitionGroup中查找SortGroup
 * *3 对于每个SortGroup，生成LogicalSort算子；
 * *4 对于SortGroup中的每个WindowGroup，生成LogicalWindow算子；
 */
public class ResolveWindowFunction implements AnalysisRuleFactory {

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
            RuleType.WINDOW_FUNCTION_FROM_SORT_AGG.build(
                logicalSort(logicalAggregate()).thenApply(ctx -> {
                    LogicalSort<LogicalAggregate<GroupPlan>> logicalSort = ctx.root;

                    List<Expression> expressionList = logicalSort.getOrderKeys().stream()
                        .map(orderKey -> orderKey.getExpr()).collect(Collectors.toList());
                    List<Window> windowList = extractWindowExpression(expressionList);

                    LogicalAggregate<GroupPlan> logicalAggregate = ctx.root.child();
                    List<NamedExpression> outputExpressions = logicalAggregate.getOutputExpressions();
                    windowList.addAll(extractWindowExpression(outputExpressions));

                    if (windowList.isEmpty()) {
                        return logicalSort;
                    }
                    return init(windowList, logicalSort);
                })
            ),
            RuleType.WINDOW_FUNCTION_FROM_SORT_PROJECT.build(
                logicalSort(logicalProject()).thenApply(ctx -> {
                    LogicalSort<LogicalProject<GroupPlan>> logicalSort = ctx.root;

                    List<Expression> expressionList = logicalSort.getOrderKeys().stream()
                        .map(orderKey -> orderKey.getExpr()).collect(Collectors.toList());
                    List<Window> windowList = extractWindowExpression(expressionList);

                    LogicalProject<GroupPlan> logicalProject = ctx.root.child();
                    List<NamedExpression> projects = logicalProject.getProjects();
                    windowList.addAll(extractWindowExpression(projects));

                    if (windowList.isEmpty()) {
                        return logicalProject;
                    }
                    return init(windowList, logicalProject);
                })
            ),
            RuleType.WINDOW_FUNCTION_FROM_PROJECT.build(
                logicalProject().thenApply(ctx -> {
                    LogicalProject<GroupPlan> logicalProject = ctx.root;
                    List<NamedExpression> projects = logicalProject.getProjects();

                    List<Window> windowList = extractWindowExpression(projects);

                    if (windowList.isEmpty()) {
                        return logicalProject;
                    }
                    return init(windowList, logicalProject);
                })
            ),
            RuleType.WINDOW_FUNCTION_FROM_AGG.build(
                logicalAggregate().thenApply(ctx -> {
                    LogicalAggregate<GroupPlan> logicalAggregate = ctx.root;
                    List<NamedExpression> outputExpressions = logicalAggregate.getOutputExpressions();

                    List<Window> windowList = extractWindowExpression(outputExpressions);

                    if (windowList.isEmpty()) {
                        return logicalAggregate;
                    }
                    return init(windowList, logicalAggregate);
                })
            ),
            RuleType.WINDOW_FUNCTION_FROM_SORT.build(
                logicalSort().thenApply(ctx -> {
                    LogicalSort<GroupPlan> logicalSort = ctx.root;
                    List<Expression> expressionList = logicalSort.getOrderKeys().stream()
                            .map(orderKey -> orderKey.getExpr()).collect(Collectors.toList());

                    List<Window> windowList = extractWindowExpression(expressionList);

                    if (windowList.isEmpty()) {
                        return logicalSort;
                    }
                    return init(windowList, logicalSort);
                })
            )
        );
    }

    private <E extends Expression> List<Window> extractWindowExpression(List<E> expressionList) {
        List<Window> windowList = expressionList.stream().filter(expression -> {
            if (expression instanceof UnboundAlias && expression.child(0) instanceof Window) {
                return true;
            }
            return false;
        }).map(expression -> (Window) expression.child(0)).collect(Collectors.toList());
        return windowList;
    }

    /**
     *  main procedure
     * @param windowList all collected window functions
     */
    private LogicalWindow init(List<Window> windowList, LogicalPlan root) {

        // todo: rewriteSmap? 只处理特定的几个函数（ntile）

        // create AnalyticInfo

        // 对于包含AnalyticInfo的selectStmt，生成相应的处理窗口的算子

        // check windowFrame
        windowList.stream().forEach(window -> checkWindowFrame(window));

        // 删掉LogicalProject中的window expr，或在后续的rewrite中消除

        // 创建windowGroup
        List<WindowFrameGroup> windowFrameGroupList = createCommonWindowFrameGroups(windowList);
        // todo: init()?
        List<OrderKeyGroup> orderKeyGroupList = createCommonOrderKeyGroups(windowFrameGroupList);
        // init(), merge()
        List<PartitionKeyGroup> partitionKeyGroupList = createCommonPartitionKeyGroups(orderKeyGroupList);
        // init(), merge(), order()

        LogicalPlan newRoot = root;
        for (PartitionKeyGroup partitionKeyGroup : partitionKeyGroupList) {
            for (OrderKeyGroup orderKeyGroup : partitionKeyGroup.groupList) {
                // 为每个OrderKeyGroup创建相应的算子，它是创建Sort算子的单位；
                newRoot = createLogicalPlanNodeForWindowFunctions(newRoot, orderKeyGroup);
            }
        }
        return (LogicalWindow) newRoot;
    }

    /* ********************************************************************************************
     * WindowFrame check and standardization
     * ******************************************************************************************** */

    /**
     * Window frame 补全和检查逻辑：
     *
     * 无window frame：
     * - rank类，up - cr
     * - 聚合类，up - uf
     * -
     */
    private void checkWindowFrame(Window window) {
        if (!window.getWindowSpec().getWindowFrame().isPresent()) {
            // todo: 根据不同聚合函数来定义不同的frame
            WindowFrame windowFrame = new WindowFrame(FrameUnitsType.ROWS,
                    FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());
            window.getWindowSpec().setWindowFrame(windowFrame);
        }

        window.getWindowSpec().getWindowFrame().ifPresent(windowFrame -> resolveFrameBoundary(windowFrame));
    }

    /**
     * if windowFrame's rightBoundary, we should complete it
     */
    private void resolveFrameBoundary(WindowFrame windowFrame) {
        if (windowFrame.getRightBoundary() != null) {
            return;
        }

        // "over( rows|range [UNBOUNDED] FOLLOWING)" is invalid.
        if (windowFrame.getLeftBoundary().getFrameBoundType().isFollowing()) {
            throw new AnalysisException("");
        }

        // "over( rows|range CURRENT ROW)" equals to "over( row|range between CURRENT ROW and CURRENT ROW)"
        // "over( rows|range [UNBOUNDED] PRECEDING)"
        //   equals to "over( row|range between [UNBOUNDED] PRECEDING and CURRENT ROW)"
        windowFrame.setRightBoundary(new FrameBoundary(FrameBoundType.CURRENT_ROW, Optional.empty()));
    }

    /* ********************************************************************************************
     * create LogicalWindow and LogicalSort
     * ******************************************************************************************** */

    private LogicalPlan createLogicalPlanNodeForWindowFunctions(LogicalPlan root, OrderKeyGroup orderKeyGroup) {
        LogicalPlan newRoot;
        // LogicalSort for orderKeys; if there exists no orderKey, newRoot = root
        newRoot = createLogicalSortNode(root, orderKeyGroup);

        // LogicalWindow for windows; at least one LogicalWindow node will be added
        for (WindowFrameGroup windowFrameGroup : orderKeyGroup.groupList) {
            newRoot = createLogicalWindow(newRoot, windowFrameGroup);
        }

        return newRoot;
    }

    private LogicalPlan createLogicalSortNode(LogicalPlan root, OrderKeyGroup orderKeyGroup) {
        // all keys that need to be sorted, which includes BOTH partitionKeys and orderKeys from this group
        List<OrderKey> keysNeedToBeSortedList = Lists.newArrayList();
        if (!orderKeyGroup.partitionKeyList.isEmpty()) {
            keysNeedToBeSortedList.addAll(orderKeyGroup.partitionKeyList.stream().map(partitionKey -> {
                // todo: haven't support isNullFirst, and its default value is true(see LogicalPlanBuilder)
                return new OrderKey(partitionKey, true, true);
            }).collect(Collectors.toList()));
        }

        if (!orderKeyGroup.orderKeyList.isEmpty()) {
            keysNeedToBeSortedList.addAll(orderKeyGroup.orderKeyList);
        }

        // add a LogicalSort node to resolve sorting requirement
        if (!keysNeedToBeSortedList.isEmpty()) {
            LogicalSort logicalSort = new LogicalSort(keysNeedToBeSortedList, root);
            return logicalSort;

            // todo: check if this group contains the sorting requirements caused by partitionKeys; if not,
            //  this sorting is consistent with the sorting processing logic caused by the normal order by clause
        }
        return root;
    }

    private LogicalWindow createLogicalWindow(LogicalPlan root, WindowFrameGroup windowFrameGroup) {
        LogicalWindow logicalWindow = new LogicalWindow(windowFrameGroup.groupList,
                windowFrameGroup.partitionKeyList, windowFrameGroup.orderKeyList, root);
        return logicalWindow;
    }

    /* ********************************************************************************************
     * WindowFunctionRelatedGroups
     * ******************************************************************************************** */

    // todo: can we simplify the following three algorithms?
    private List<WindowFrameGroup> createCommonWindowFrameGroups(List<Window> windowList) {
        List<WindowFrameGroup> windowFrameGroupList = Lists.newArrayList();
        for (int i = 0; i < windowList.size(); i++) {
            Window window = windowList.get(i);

            boolean matched = false;
            for (WindowFrameGroup windowFrameGroup : windowFrameGroupList) {
                if (windowFrameGroup.isCompatible(window)) {
                    windowFrameGroup.addGroupMember(window);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                windowFrameGroupList.add(new WindowFrameGroup(window));
            }
        }
        return windowFrameGroupList;
    }

    private List<OrderKeyGroup> createCommonOrderKeyGroups(List<WindowFrameGroup> windowFrameGroupList) {
        List<OrderKeyGroup> orderKeyGroupList = Lists.newArrayList();

        for (WindowFrameGroup windowFrameGroup : windowFrameGroupList) {
            boolean matched = false;
            for (OrderKeyGroup orderKeyGroup : orderKeyGroupList) {
                if (orderKeyGroup.isCompatible(windowFrameGroup)) {
                    orderKeyGroup.addGroupMember(windowFrameGroup);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                orderKeyGroupList.add(new OrderKeyGroup(windowFrameGroup));
            }
        }
        return orderKeyGroupList;
    }

    private List<PartitionKeyGroup> createCommonPartitionKeyGroups(List<OrderKeyGroup> orderKeyGroupList) {
        List<PartitionKeyGroup> partitionKeyGroupList = Lists.newArrayList();

        for (OrderKeyGroup orderKeyGroup : orderKeyGroupList) {
            boolean matched = false;
            for (PartitionKeyGroup partitionKeyGroup : partitionKeyGroupList) {
                if (partitionKeyGroup.isCompatible(orderKeyGroup)) {
                    partitionKeyGroup.addGroupMember(orderKeyGroup);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                partitionKeyGroupList.add(new PartitionKeyGroup(orderKeyGroup));
            }
        }
        return partitionKeyGroupList;
    }

    /**
     * Window Functions that have common PartitionKeys, OrderKeys and WindowFrame
     */
    private static class WindowFrameGroup extends WindowFunctionRelatedGroup<Window> {

        // Group内共用的标识性信息 要不要改为commonXXXList？
        public final List<Expression> partitionKeyList;
        public final List<OrderKey> orderKeyList;
        public final WindowFrame windowFrame;

        // 物理信息、outputSlot信息

        public WindowFrameGroup(Window window) {
            // maybe OrElse(null)?
            partitionKeyList = window.getWindowSpec().getPartitionKeyList().orElse(Lists.newArrayList());
            orderKeyList = window.getWindowSpec().getOrderKeyList().orElse(Lists.newArrayList());
            windowFrame = window.getWindowSpec().getWindowFrame().orElse(null);

            groupList.add(window);
        }

        @Override
        public boolean isCompatible(Window window) {
            // The comparison of PartitionKey is irrelevant to key's order,
            // but not in OrderKey' comparison.

            List<Expression> otherPartitionKeyList = window.getWindowSpec().getPartitionKeyList().orElse(null);
            List<OrderKey> otherOrderKeyList = window.getWindowSpec().getOrderKeyList().orElse(null);
            WindowFrame otherWindowFrame = window.getWindowSpec().getWindowFrame().orElse(null);

            // this function is absolutely equals to Expr.equalSets()
            if (CollectionUtils.isEqualCollection(partitionKeyList, otherPartitionKeyList)
                    && orderKeyList.equals(otherOrderKeyList)) {
                if ((windowFrame == null && otherWindowFrame == null) || windowFrame.equals(otherWindowFrame)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Window Functions that have common PartitionKeys and OrderKeys.
     */
    private static class OrderKeyGroup extends WindowFunctionRelatedGroup<WindowFrameGroup> {

        private final List<Expression> partitionKeyList;
        private final List<OrderKey> orderKeyList;

        public OrderKeyGroup(WindowFrameGroup windowFrameGroup) {
            partitionKeyList = windowFrameGroup.partitionKeyList;
            orderKeyList = windowFrameGroup.orderKeyList;
            groupList.add(windowFrameGroup);
        }

        @Override
        public boolean isCompatible(WindowFrameGroup windowFrameGroup) {
            return CollectionUtils.isEqualCollection(partitionKeyList, windowFrameGroup.partitionKeyList)
                && orderKeyList.equals(windowFrameGroup.orderKeyList);
        }
    }

    /**
     * Window Functions that have common PartitionKeys.
     */
    private static class PartitionKeyGroup extends WindowFunctionRelatedGroup<OrderKeyGroup> {
        public final List<Expression> partitionKeyList;

        public PartitionKeyGroup(OrderKeyGroup orderKeyGroup) {
            partitionKeyList = orderKeyGroup.partitionKeyList;
            groupList.add(orderKeyGroup);
        }

        @Override
        public boolean isCompatible(OrderKeyGroup orderKeyGroup) {
            return CollectionUtils.isEqualCollection(partitionKeyList, orderKeyGroup.partitionKeyList);
        }
    }

    private abstract static class WindowFunctionRelatedGroup<G> {

        List<G> groupList = Lists.newArrayList();

        public abstract boolean isCompatible(G group);

        public void addGroupMember(G group) {
            groupList.add(group);
        }
    }

}
