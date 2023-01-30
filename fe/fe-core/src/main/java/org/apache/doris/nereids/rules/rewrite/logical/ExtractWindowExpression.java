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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Window;

import java.util.List;
import java.util.stream.Collectors;

/**
 * extract window expressions from LogicalWindow.outputExpressions
 */
public class ExtractWindowExpression extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalWindow().when(window -> window.getWindowExpressions() == null).then(window -> {
            List<NamedExpression> windowExpressions = window.getOutputExpressions().stream()
                    .filter(expr -> expr.anyMatch(Window.class::isInstance))
                    .collect(Collectors.toList());
            return window.withWindowExpressions(windowExpressions);
        }).toRule(RuleType.EXTRACT_WINDOW_EXPRESSIONS);
    }
}