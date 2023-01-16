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
 * Check and standardize Window expression:
 *
 * step 1: checkWindowFrameBeforeFunc():
 *  general checking for WindowFrame, including check OrderKeyList, set default right boundary, check offset if exists,
 *  check correctness of boundaryType
 * step 2: checkWindowFunction():
 *  check window function, and different function has different checking rules .
 *  If window frame not exits, set a unique default window frame according to their function type.
 * step 3: checkWindowAfterFunc():
 *  reverse window if necessary (just for first_value() and last_value()), and add a general default
 *  window frame (RANGE between UNBOUNDED PRECEDING and CURRENT ROW)
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
