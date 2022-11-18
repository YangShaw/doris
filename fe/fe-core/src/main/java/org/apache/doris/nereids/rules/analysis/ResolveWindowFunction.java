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

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.List;
import java.util.stream.Collectors;

public class ResolveWindowFunction extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return RuleType.RESOLVE_WINDOW_FUNCTION.build(
            logicalProject().thenApply(ctx -> {
                LogicalProject<GroupPlan> logicalProject = ctx.root;
                List<NamedExpression> projects = logicalProject.getProjects();
                List<Window> windowList = projects.stream().filter(project -> {
                    if (project instanceof UnboundAlias && project.child(0) instanceof Window) {
                        return true;
                    }
                    return false;
                }).map(project -> (Window) project.child(0)).collect(Collectors.toList());

                return null;
            })
        );
    }

    /**
     * 核心目标：创建逻辑节点，来维护窗口函数相关的信息。窗口函数对排序有要求，因此也需要增加Sort相关的算子；
     * 为了避免重复排序（分区字段也相当于排序），需要对不同窗口函数的信息进行分析，合并同类项。
     *
     * 标准化部分
     * 1 对每一个Window做检查
     * 2 补充窗口，进行标准化，不同的窗口函数有不同的标准化规则（不确定要在几步中进行）
     *
     * 合并同类项部分
     * 1 计算三种Group:
     *      WindowFrameGroup:分区、排序、窗口都相同
     *      OrderKeyGroup: 分区、排序相同
     *      PartitionKeyGroup: 分区相同
     * 2 在PartitionGroup中查找SortGroup
     * 3 对于每个SortGroup，生成LogicalSort算子；
     * 4 对于SortGroup中的每个WindowGroup，生成LogicalWindow算子；
     */

    /**
     *  main procedure
     * @param windowList all collected window functions
     */
    private void init(List<Window> windowList) {

        // todo: rewriteSmap? 只处理特定的几个函数（ntile）

        // create AnalyticInfo

        // 对于包含AnalyticInfo的selectStmt，生成相应的处理窗口的算子

        // 创建windowGroup
    }

    /**
     * Window Functions that have common PartitionKeys, OrderKeys and WindowFrame
     */
    private static class WindowFrameGroup {

        // Group内共用的标识性信息 要不要改为commonXXXList？
        public final List<Expression> partitionKeyList;
        public final List<OrderKey> orderKeyList;
        public final WindowFrame windowFrame;

        // Group内信息
        public final List<Window> windowList = Lists.newArrayList();

        // 物理信息、outputSlot信息

        public WindowFrameGroup(Window window) {
            // maybe OrElse(null)?
            partitionKeyList = window.getWindowSpec().getPartitionKeyList().orElse(Lists.newArrayList());
            orderKeyList = window.getWindowSpec().getOrderKeyList().orElse(Lists.newArrayList());
            windowFrame = window.getWindowSpec().getWindowFrame().get();

            windowList.add(window);
        }

        public boolean isCompatible(Window window) {
            // The comparison of PartitionKey is irrelevant to key's order,
            // but not in OrderKey' comparison.

            List<Expression> otherPartitionKeyList = window.getWindowSpec().getPartitionKeyList().orElse(null);
            List<OrderKey> otherOrderKeyList = window.getWindowSpec().getOrderKeyList().orElse(null);
            WindowFrame otherWindowFrame = window.getWindowSpec().getWindowFrame().orElse(null);

            // this function is absolutely equals to Expr.equalSets()
            // 本身需要有空判断
            if (CollectionUtils.isEqualCollection(partitionKeyList, otherPartitionKeyList)
                && orderKeyList.equals(otherOrderKeyList)) {
                if ((windowFrame == null && otherWindowFrame == null) || windowFrame.equals(otherWindowFrame)) {
                    return true;
                }
            }
            return false;
        }

        public void addGroupMember(Window window) {
            windowList.add(window);
        }
    }

    /**
     * Window Functions that have common PartitionKeys and OrderKeys.
     */
    private static class OrderKeyGroup {

        private final List<Expression> partitionKeyList;
        private final List<OrderKey> orderKeyList;

        private List<WindowFrameGroup> windowFrameGroupList = Lists.newArrayList();

        public OrderKeyGroup(Window window) {
            partitionKeyList = window.getWindowSpec().getPartitionKeyList().orElse(Lists.newArrayList());
            orderKeyList = window.getWindowSpec().getOrderKeyList().orElse(Lists.newArrayList());
        }

        public OrderKeyGroup(WindowFrameGroup windowFrameGroup) {
            partitionKeyList = windowFrameGroup.partitionKeyList;
            orderKeyList = windowFrameGroup.orderKeyList;
            windowFrameGroupList.add(windowFrameGroup);
        }

        public boolean isCompatible(WindowFrameGroup windowFrameGroup) {
            return CollectionUtils.isEqualCollection(partitionKeyList, windowFrameGroup.partitionKeyList)
                && orderKeyList.equals(windowFrameGroup.orderKeyList);
        }

        public void addGroupMember(WindowFrameGroup windowFrameGroup) {
            windowFrameGroupList.add(windowFrameGroup);
        }

    }

    /**
     * Window Functions that have common PartitionKeys.
     */
    private static class PartitionKeyGroup {
        public final List<Expression> partitionKeyList;

        public List<OrderKeyGroup> orderKeyGroupList = Lists.newArrayList();

        public PartitionKeyGroup(OrderKeyGroup orderKeyGroup) {
            partitionKeyList = orderKeyGroup.partitionKeyList;
            orderKeyGroupList.add(orderKeyGroup);
        }

        public boolean isCompatible(OrderKeyGroup orderKeyGroup) {
            return CollectionUtils.isEqualCollection(partitionKeyList, orderKeyGroup.partitionKeyList);
        }

        public void addGroupMember(OrderKeyGroup orderKeyGroup) {
            orderKeyGroupList.add(orderKeyGroup);
        }
    }

}
