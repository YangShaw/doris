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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WithClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Context used for CTE analysis and register
 */
public class CTEContext {

    private Map<String, LogicalPlan> withQueries;

    public CTEContext() {
        this.withQueries = new HashMap<>();
    }

    public boolean containsCTE(String cteName) {
        return withQueries.containsKey(cteName);
    }

    public void addCTE(String cteName, LogicalPlan ctePlan) {
        withQueries.put(cteName, ctePlan);
    }

    public Optional<LogicalPlan> findCTE(String name) {
        return Optional.ofNullable(withQueries.get(name));
    }

    /**
     * register with queries in CTEContext
     * @param withClause includes with query
     * @param parentContext parent CascadesContext
     */
    public void registerWithQuery(WithClause withClause, CascadesContext parentContext) {
        String name = withClause.getName();
        if (withQueries.containsKey(name)) {
            throw new AnalysisException("Name " + name + " of CTE cannot be used more than once.");
        }

        CascadesContext cascadesContext = new Memo(withClause.getQuery())
                .newCascadesContext(parentContext.getStatementContext());
        cascadesContext.newAnalyzer(this).analyze();

        LogicalPlan analyzedPlan = (LogicalPlan) cascadesContext.getMemo().copyOut(false);
        if (withClause.getColumnAliases().isPresent()) {
            checkColumnAlias(withClause, analyzedPlan.getOutput());
        }
        withQueries.put(name, analyzedPlan);
        // withQueries.put(name, withClause.getQuery());
    }

    /**
     * register with queries in CTEContext
     * @param withClause includes with query
     * @param statementContext global statementContext
     */
    public void registerWithQuery(WithClause withClause, StatementContext statementContext) {
        String name = withClause.getName();
        if (withQueries.containsKey(name)) {
            throw new AnalysisException("Name " + name + " of CTE cannot be used more than once.");
        }

        CascadesContext cascadesContext = new Memo(withClause.getQuery())
                .newCascadesContext(statementContext);
        cascadesContext.newAnalyzer(this).analyze();

        LogicalPlan analyzedPlan = (LogicalPlan) cascadesContext.getMemo().copyOut(false);
        if (withClause.getColumnAliases().isPresent()) {
            checkColumnAlias(withClause, analyzedPlan.getOutput());
            analyzedPlan = withColumnAliases(analyzedPlan, withClause);
        }
        withQueries.put(name, analyzedPlan);
        // withQueries.put(name, withClause.getQuery());
    }

    private LogicalPlan withColumnAliases(LogicalPlan queryPlan, WithClause withClause) {
        List<Slot> outputSlots = queryPlan.getOutput();
        List<String> columnAliases = withClause.getColumnAliases().get();
        for (int i = 0; i < outputSlots.size(); i++) {

        }

        List<NamedExpression> projects = IntStream.range(0, outputSlots.size())
                .mapToObj(i -> new Alias(outputSlots.get(i), columnAliases.get(i)))
                .collect(Collectors.toList());
        return new LogicalProject<>(projects, queryPlan.getGroupExpression(),
                Optional.ofNullable(queryPlan.getLogicalProperties()), queryPlan);
    }

    // todo: move these validate operation to related job.
    private void checkColumnAlias(WithClause withClause) {
        List<Slot> outputSlots = withClause.getQuery().getOutput();
        this.checkColumnAlias(withClause, outputSlots);
    }

    private void checkColumnAlias(WithClause withClause, List<Slot> outputSlots) {
        List<String> columnAlias = withClause.getColumnAliases().get();
        if (columnAlias.size() != outputSlots.size()) {
            throw new AnalysisException("WITH-clause '" + withClause.getName() + "' returns " + columnAlias.size()
                + " columns, but " + outputSlots.size() + " labels were specified. The number of column labels must "
                + "be smaller or equal to the number of returned columns.");
        }

        Set<String> names = new HashSet<>();
        columnAlias.stream().forEach(alias -> {
            if (names.contains(alias.toLowerCase())) {
                throw new AnalysisException("Duplicated CTE column alias: '" + alias.toLowerCase()
                    + "' in CTE " + withClause.getName());
            }
            names.add(alias);
        });
    }
}
