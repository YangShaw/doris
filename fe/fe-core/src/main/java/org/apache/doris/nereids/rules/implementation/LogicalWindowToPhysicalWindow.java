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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.logical.CheckAndStandardizeWindowFunctionAndFrame;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeWindow;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation rule that convert logical window to physical window.
 */
@DependsRules({
    NormalizeWindow.class,
    CheckAndStandardizeWindowFunctionAndFrame.class
})
public class LogicalWindowToPhysicalWindow extends OneImplementationRuleFactory {

    @Override
    public Rule build() {

        return RuleType.LOGICAL_WINDOW_TO_PHYSICAL_WINDOW_RULE.build(
            logicalWindow().when(LogicalWindow::isNormalized)
                .when(LogicalWindow::isChecked)
                .thenApply(ctx -> resolveWindow(ctx.cascadesContext, ctx.root))
        );
    }

    /**
     *  main procedure
     */
    private PhysicalWindow resolveWindow(CascadesContext ctx, LogicalWindow<GroupPlan> logicalWindow) {
        List<NamedExpression> windowList = logicalWindow.getWindowExpressions();

        /////////// create three kinds of groups and compute tupleSize of each
        // windowFrameGroup
        List<WindowFrameGroup> windowFrameGroupList = createCommonWindowFrameGroups(windowList);

        // orderKeyGroup
        List<OrderKeyGroup> orderKeyGroupList = createCommonOrderKeyGroups(windowFrameGroupList);
        mergeOrderKeyGroups(orderKeyGroupList);

        // partitionKeyGroup
        List<PartitionKeyGroup> partitionKeyGroupList = createCommonPartitionKeyGroups(orderKeyGroupList);
        mergePartitionKeyGroups(partitionKeyGroupList);

        // sort groups
        sortPartitionKeyGroups(partitionKeyGroupList);

        // eliminate LogicalWindow, and replaced it with new LogicalWindow and LogicalSort
        Plan newRoot = logicalWindow.child();
        for (PartitionKeyGroup partitionKeyGroup : partitionKeyGroupList) {
            for (OrderKeyGroup orderKeyGroup : partitionKeyGroup.groupList) {
                // create LogicalSort for each OrderKeyGroup;
                // in OrderKeyGroup, create LogicalWindow for each WindowFrameGroup
                newRoot = createLogicalPlanNodeForWindowFunctions(newRoot, orderKeyGroup, logicalWindow, ctx);
            }
        }
        return (PhysicalWindow) newRoot;
    }

    /* ********************************************************************************************
     * create PhyscialWindow and PhysicalSort
     * ******************************************************************************************** */

    private Plan createLogicalPlanNodeForWindowFunctions(Plan root, OrderKeyGroup orderKeyGroup,
                                                                LogicalWindow logicalWindow, CascadesContext ctx) {
        // PhysicalSort node for orderKeys; if there exists no orderKey, newRoot = root
        Plan newRoot = createPhysicalSortNode(root, orderKeyGroup, ctx);

        // PhysicalWindow node for window functions; at least one PhysicalWindow node will be added
        for (WindowFrameGroup windowFrameGroup : orderKeyGroup.groupList) {
            newRoot = createPhysicalWindow(newRoot, windowFrameGroup, logicalWindow);
        }

        return newRoot;
    }

    private Plan createPhysicalSortNode(Plan root, OrderKeyGroup orderKeyGroup, CascadesContext ctx) {
        // all keys that need to be sorted, which includes BOTH partitionKeys and orderKeys from this group
        List<OrderKey> keysNeedToBeSortedList = Lists.newArrayList();

        // todo: used as SortNode.isAnalyticSort, but we haven't add it in LogicalSort
        boolean isAnalyticSort = false;
        if (!orderKeyGroup.partitionKeyList.isEmpty()) {
            keysNeedToBeSortedList.addAll(orderKeyGroup.partitionKeyList.stream().map(partitionKey -> {
                // todo: haven't support isNullFirst, and its default value is false(see AnalyticPlanner#line403,
                //  but in LogicalPlanBuilder, its default value is true)
                return new OrderKey(partitionKey, true, false);
            }).collect(Collectors.toList()));
            isAnalyticSort = true;
        }

        if (!orderKeyGroup.orderKeyList.isEmpty()) {
            keysNeedToBeSortedList.addAll(orderKeyGroup.orderKeyList);
        }

        // add a PhysicalQuickSort node to resolve sorting requirement
        if (!keysNeedToBeSortedList.isEmpty()) {
            LogicalSort logicalSort = new LogicalSort(keysNeedToBeSortedList, root);
            PhysicalQuickSort physicalQuickSort = (PhysicalQuickSort) new LogicalSortToPhysicalQuickSort()
                    .build()
                    .transform(logicalSort, ctx)
                    .get(0);
            return physicalQuickSort.withAnalyticSort(isAnalyticSort);

            // todo: check if this group contains the sorting requirements caused by partitionKeys; if not,
            //  this sorting is consistent with the sorting processing logic caused by the normal order by clause
        }
        // no Order-related keys, so no need for LogicalSort
        return root;
    }

    private PhysicalWindow createPhysicalWindow(Plan root, WindowFrameGroup windowFrameGroup,
                                                LogicalWindow logicalWindow) {
        // todo: partitionByEq and orderByEq
        return new PhysicalWindow<>(
                logicalWindow.getOutputExpressions(),
                logicalWindow.getWindowExpressions(),
                windowFrameGroup,
                logicalWindow.getLogicalProperties(),
                root);
    }

    /* ********************************************************************************************
     * WindowFunctionRelatedGroups
     * ******************************************************************************************** */

    // todo: can we simplify the following three algorithms?
    private List<WindowFrameGroup> createCommonWindowFrameGroups(List<NamedExpression> windowList) {
        List<WindowFrameGroup> windowFrameGroupList = Lists.newArrayList();
        for (int i = 0; i < windowList.size(); i++) {
            NamedExpression windowAlias = windowList.get(i);

            boolean matched = false;
            for (WindowFrameGroup windowFrameGroup : windowFrameGroupList) {
                if (windowFrameGroup.isCompatible(windowAlias)) {
                    windowFrameGroup.addGroupMember(windowAlias);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                windowFrameGroupList.add(new WindowFrameGroup(windowAlias));
            }
        }

        for (WindowFrameGroup wfg : windowFrameGroupList) {
            wfg.setTupleSize(wfg.groupList.stream().mapToInt(window -> window.child(0).getDataType().width()).sum());
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

        for (OrderKeyGroup okg : orderKeyGroupList) {
            okg.setTupleSize(okg.getGroupList().stream().mapToInt(WindowFrameGroup::getTupleSize).sum());
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

        for (PartitionKeyGroup pkg : partitionKeyGroupList) {
            pkg.setTupleSize(pkg.getGroupList().stream().mapToInt(OrderKeyGroup::getTupleSize).sum());
        }

        return partitionKeyGroupList;
    }

    private void mergeOrderKeyGroups(List<OrderKeyGroup> orderKeyGroupList) {
        boolean merged = true;

        while (merged) {
            merged = false;
            for (OrderKeyGroup okg1 : orderKeyGroupList) {
                for (OrderKeyGroup okg2 : orderKeyGroupList) {
                    if (okg1 != okg2 && okg2.isPrefixOf(okg1)) {
                        // okg2 ∈ okg1
                        okg1.absorb(okg2);
                        orderKeyGroupList.remove(okg2);
                        merged = true;
                        break;
                    }
                }
                if (merged) {
                    break;
                }
            }
        }
    }

    private void mergePartitionKeyGroups(List<PartitionKeyGroup> partitionKeyGroupList) {
        boolean merged = true;

        while (merged) {
            merged = false;
            for (PartitionKeyGroup pkg1 : partitionKeyGroupList) {
                for (PartitionKeyGroup pkg2 : partitionKeyGroupList) {
                    if (pkg1 != pkg2) {
                        // todo:
                        pkg1.absorb(pkg2);
                        partitionKeyGroupList.remove(pkg2);
                        merged = true;
                        break;
                    }
                }
                if (merged) {
                    break;
                }
            }
        }
    }

    /**
     * [This comment references AnalyticPlanner.orderGroups()]
     *
     * Order partition groups (and the sort groups within them) by increasing
     * totalOutputTupleSize. This minimizes the total volume of data that needs to be
     * repartitioned and sorted.
     * Also move the non-partitioning partition group to the end.
     */
    private void sortPartitionKeyGroups(List<PartitionKeyGroup> partitionKeyGroupList) {

        PartitionKeyGroup noPartition = null;
        // after createCommonPartitionKeyGroups(), there will be at most one empty partition.
        for (PartitionKeyGroup pkg : partitionKeyGroupList) {
            if (pkg.partitionKeyList.isEmpty()) {
                noPartition = pkg;
                partitionKeyGroupList.remove(noPartition);
                break;
            }
        }

        if (noPartition != null) {
            partitionKeyGroupList.remove(noPartition);
        }

        partitionKeyGroupList.sort((pkg1, pkg2) ->
                Integer.compare(pkg1.getTupleSize() - pkg2.getTupleSize(), 0)
        );

        if (noPartition != null) {
            partitionKeyGroupList.add(noPartition);
        }

        for (PartitionKeyGroup pkg : partitionKeyGroupList) {
            sortOrderKeyGroups(pkg.getGroupList());
        }

    }

    private void sortOrderKeyGroups(List<OrderKeyGroup> orderKeyGroupList) {
        orderKeyGroupList.sort((okg1, okg2) ->
                Integer.compare(okg1.getTupleSize() - okg2.getTupleSize(), 0)
        );

        for (OrderKeyGroup okg : orderKeyGroupList) {
            sortWindowFrameGroups(okg.getGroupList());
        }
    }

    private void sortWindowFrameGroups(List<WindowFrameGroup> windowFrameGroupList) {
        windowFrameGroupList.sort((wfg1, wfg2) ->
                Integer.compare(wfg1.getTupleSize() - wfg2.getTupleSize(), 0)
        );
    }

    /**
     * Window Functions that have common PartitionKeys, OrderKeys and WindowFrame
     */
    public static class WindowFrameGroup extends WindowFunctionRelatedGroup<NamedExpression> {

        private final List<Expression> partitionKeyList;
        private final List<OrderKey> orderKeyList;
        private final WindowFrame windowFrame;

        public WindowFrameGroup(NamedExpression windowAlias) {
            Window window = (Window) (windowAlias.child(0));
            partitionKeyList = window.getPartitionKeyList().orElse(Lists.newArrayList());
            orderKeyList = window.getOrderKeyList().orElse(Lists.newArrayList());
            windowFrame = window.getWindowFrame().orElse(null);
            groupList.add(windowAlias);
        }

        @Override
        public void addGroupMember(NamedExpression windowAlias) {
            groupList.add(windowAlias);
        }

        @Override
        public boolean isCompatible(NamedExpression windowAlias) {
            // The comparison of PartitionKey is irrelevant to key's order,
            // but not in OrderKey' comparison.
            Window window = (Window) (windowAlias.child(0));

            List<Expression> otherPartitionKeyList = window.getPartitionKeyList().orElse(null);
            List<OrderKey> otherOrderKeyList = window.getOrderKeyList().orElse(null);
            WindowFrame otherWindowFrame = window.getWindowFrame().orElse(null);

            // for PartitionKeys, we don't care about the order of each key, so we use isEqualCollection() to compare
            // whether these two lists have same keys; but for OrderKeys, the order of each key also make sense, so
            // we use equals() to compare both the elements and their order in these two lists.
            if (CollectionUtils.isEqualCollection(partitionKeyList, otherPartitionKeyList)
                    && orderKeyList.equals(otherOrderKeyList)) {
                // CollectionUtils.isEqualCollection() is absolutely equals to Expr.equalSets()
                if ((windowFrame == null && otherWindowFrame == null) || windowFrame.equals(otherWindowFrame)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("PartitionKeys: ").append(partitionKeyList.stream()
                    .map(Expression::toString)
                    .collect(Collectors.joining(", ", "", ", ")));

            sb.append("OrderKeys: ").append(orderKeyList.stream()
                    .map(OrderKey::toString)
                    .collect(Collectors.joining(", ", "", ", ")));

            sb.append("WindowFrame: ").append(windowFrame);
            return sb.toString();
        }

        public List<Expression> getPartitionKeyList() {
            return partitionKeyList;
        }

        public List<OrderKey> getOrderKeyList() {
            return orderKeyList;
        }

        public WindowFrame getWindowFrame() {
            return windowFrame;
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

        /**
         *  check if this okg isPrefixOf other, which means that other okg can cover this
         */
        public boolean isPrefixOf(OrderKeyGroup otherOkg) {
            if (orderKeyList.size() > otherOkg.orderKeyList.size()) {
                return false;
            }
            if (!CollectionUtils.isEqualCollection(partitionKeyList, otherOkg.partitionKeyList)) {
                return false;
            }

            for (int i = 0; i < orderKeyList.size(); i++) {
                if (!orderKeyList.get(i).equals(otherOkg.orderKeyList.get(i))) {
                    return false;
                }
            }
            return true;
        }

        public void absorb(OrderKeyGroup otherOkg) {
            groupList.addAll(otherOkg.groupList);
        }

    }

    /**
     * Window Functions that have common PartitionKeys.
     */
    private static class PartitionKeyGroup extends WindowFunctionRelatedGroup<OrderKeyGroup> {
        public List<Expression> partitionKeyList;

        public PartitionKeyGroup(OrderKeyGroup orderKeyGroup) {
            partitionKeyList = orderKeyGroup.partitionKeyList;
            groupList.add(orderKeyGroup);
        }

        @Override
        public boolean isCompatible(OrderKeyGroup orderKeyGroup) {
            return CollectionUtils.isEqualCollection(partitionKeyList, orderKeyGroup.partitionKeyList);
        }

        /**
         * absorb other into this:
         * - partitionKeyList will be the intersection of this two group
         * - groupList(orderKeyGroup) will be the union of this two group
         */
        public void absorb(PartitionKeyGroup otherPkg) {
            List<Expression> intersectPartitionKeys = partitionKeyList.stream()
                    .filter(expression -> otherPkg.partitionKeyList.contains(expression))
                    .collect(Collectors.toList());
            partitionKeyList = intersectPartitionKeys;
            groupList.addAll(otherPkg.groupList);
        }
    }

    private abstract static class WindowFunctionRelatedGroup<G> {

        protected List<G> groupList = Lists.newArrayList();

        protected int tupleSize;

        public abstract boolean isCompatible(G group);

        public void addGroupMember(G group) {
            groupList.add(group);
        }

        public List<G> getGroupList() {
            return groupList;
        }

        public int getTupleSize() {
            return tupleSize;
        }

        public void setTupleSize(int tupleSize) {
            this.tupleSize = tupleSize;
        }
    }
}
