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
import org.apache.doris.nereids.rules.analysis.WindowFunctionChecker;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import java.util.List;
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
public class CheckAndStandardizeWindowFunctionAndFrame extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return RuleType.CHECK_AND_STANDARDIZE_WINDOW_FUNCTION_AND_FRAME.build(
            logicalWindow().whenNot(LogicalWindow::isChecked).then(logicalWindow ->
                checkAndStandardize(logicalWindow))
        );
    }

    /**
     *  main procedure
     */
    private LogicalWindow checkAndStandardize(LogicalWindow<GroupPlan> logicalWindow) {

        List<NamedExpression> windowList = logicalWindow.getWindowExpressions();

        windowList = windowList.stream().map(windowAlias -> {
            Window window = (Window) windowAlias.child(0);
            WindowFunctionChecker checker = new WindowFunctionChecker(window);
            checker.checkWindowFrameBeforeFunc();
            checker.checkWindowFunction();
            checker.checkWindowAfterFunc();
            return (Alias) windowAlias.withChildren(checker.getWindow());
        }).collect(Collectors.toList());

        // todo: replace window exprs in outputExpressions with standardized windowExpressions
        return logicalWindow.withChecked(logicalWindow.getOutputExpressions(), windowList, logicalWindow.child());
    }
}
