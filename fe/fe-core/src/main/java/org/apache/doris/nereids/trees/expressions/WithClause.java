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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.List;
import java.util.Optional;

/**
 * One withClause of CTE
 */
public class WithClause extends Expression {

    private final String name;
    private final LogicalPlan query;

    private final Optional<List<String>> columnNames;

    public WithClause(String name, LogicalPlan query, Optional<List<String>> columnNames) {
        this.name = name;
        this.query = query;
        this.columnNames = columnNames;
    }

    public String getName() {
        return name;
    }

    public LogicalPlan getQuery() {
        return query;
    }

    public Optional<List<String>> getColumnNames() {
        return columnNames;
    }

    @Override
    public String toSql() {
        return null;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWithClause(this, context);
    }

}
