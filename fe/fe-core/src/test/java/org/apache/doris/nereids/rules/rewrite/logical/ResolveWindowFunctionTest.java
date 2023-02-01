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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class ResolveWindowFunctionTest extends TestWithFeService implements PatternMatchSupported {

    public final String unifiedAlias = "uni";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE IF NOT EXISTS `supplier` (\n"
                + "  `s_suppkey` int(11) NOT NULL COMMENT \"\",\n"
                + "  `s_name` varchar(26) NOT NULL COMMENT \"\",\n"
                + "  `s_address` varchar(26) NOT NULL COMMENT \"\",\n"
                + "  `s_city` varchar(11) NOT NULL COMMENT \"\",\n"
                + "  `s_nation` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `s_region` varchar(13) NOT NULL COMMENT \"\",\n"
                + "  `s_phone` varchar(16) NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`s_suppkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"colocate_with\" = \"groupa4\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"DEFAULT\"\n"
                + ")");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    /* ********************************************************************************************
     * Test WindowFrame and different WindowFunction
     * ******************************************************************************************** */

    public void testRankLikeFunctions() {
        String rank = "SELECT rank() over() as uni FROM supplier";
        String rank2 = "SELECT rank() over(ROWS BETWEEN unbounded preceding and current row) as uni FROM supplier";
        String denseRank = "SELECT dense_rank() over() as uni FROM supplier";
        String rowNumber = "SELECT row_number() over() as uni FROM supplier";
        List<String> sqls = ImmutableList.of(
                rank, rank2, denseRank, rowNumber
        );
        List<BoundFunction> functions = ImmutableList.of(
            new Rank(), new Rank(), new DenseRank(), new RowNumber()
        );

        WindowFrame windowFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newCurrentRowBoundary());

        for (int i = 0; i < sqls.size(); i++) {
            Window window = new Window(functions.get(i), Optional.empty(), Optional.empty(),
                    Optional.of(windowFrame));
            Alias alias = new Alias(new ExprId(7), window, unifiedAlias);

            System.out.println(functions.get(i).toSql() + " " + functions.get(i).toString());
            System.out.println(window.toSql());

            PlanChecker.from(connectContext)
                    .analyze(sqls.get(i))
                    .applyTopDown(new ExtractWindowExpression())
                    .applyTopDown(new CheckAndStandardizeWindowFunctionAndFrame())
                    .matches(
                        logicalWindow()
                            .when(FieldChecker.check("windowExpressions", ImmutableList.of(alias)))
                    );
        }

        /*Window window = new Window(new Rank(), windowSpec);
        Alias alias = new Alias(new ExprId(7), window, new Window(new Rank()).toSql());
        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
        connectContext.getSessionVariable().setEnableNereidsTrace(true);
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyTopDown(new ExtractWindowExpression())
                .matches(
                    logicalWindow()
                    .when(FieldChecker.check("windowExpressions", ImmutableList.of(alias)))
                );*/
    }

    public void testFirstOrLastValue() {
        // String first_value = "SELECT first_value() over() FROM supplier";
        // String first_value2 = "SELECT first_value(s_suppkey) over(ORDER BY s_suppkey ROWS BETWEEN 3 following and unbounded following) FROM supplier";
    }

    @Test
    public void testAnalyze() {
        // String sql = "SELECT sum(s_suppkey) OVER(PARTITION BY s_nation ORDER BY s_name) FROM supplier";
        // String sql2 = "SELECT s_city, sum(s_suppkey) FROM supplier GROUP BY s_city ORDER BY s_city limit 10";
        // String sql2 = "SELECT s_city FROM supplier GROUP BY s_city ORDER BY s_city limit 10";
        // String sql2 = "SELECT s_city FROM supplier ORDER BY s_city limit 10";
        // String sql2 = "SELECT s_city FROM supplier ORDER BY s_city limit 10";
        String sql = "select count(1) from (select s_suppkey from supplier order by s_suppkey desc) t";
        PlanChecker.from(connectContext).checkPlannerResult(sql);
    }

    public void testWindowFunctionChecker() {
        // String rowNumber = "SELECT row_number() over() FROM supplier";

        // String rank = "SELECT rank() over() FROM supplier";
        String lag = "SELECT lag(s_suppkey, 1, 2) over() FROM supplier";
        PlanChecker.from(connectContext).checkPlannerResult(lag);
    }

    @Test
    public void testWindowGroup() {
        // String sql = "SELECT s_city, row_number() over(PARTITION BY s_address ORDER BY s_nation) FROM supplier";

        // String sql2 = "select s_city, row_number() over(PARTITION BY s_address ORDER BY s_nation) from supplier";
        // String sql = "select s_suppkey+2, rank() over(partition by s_suppkey+1 order by s_suppkey+3) from supplier";
        String sql2 = "select *, rank() over() from supplier";

        // select k4, k10 from (select k4, k10 from test order by 1, 2 limit 1000000) as i order by 1, 2 limit 1000
        PlanChecker.from(connectContext).checkPlannerResult(sql2);
    }

    @Test
    public void testUnit() {
        // String sql = "select regexp_replace(s_city, s_city, s_city) from supplier order by s_city, s_city, s_city";
        String sql3 = "select aes_decrypt(s_city, s_city, s_city) from supplier order by s_city, s_city, s_city";

        // String sql = "SELECT s_city, row_number() over(PARTITION BY s_address ORDER BY s_nation) FROM supplier";
        // String sql2 = "select s_city as s_address from supplier order by s_city, s_address, s_suppkey, s_nation";
        PlanChecker.from(connectContext).checkPlannerResult(sql3);
    }

    public void test() {
        String sql = "SELECT s_suppkey, count(*) FROM supplier GROUP BY s_suppkey ORDER BY s_suppkey";
        PlanChecker.from(connectContext).checkPlannerResult(sql);
    }
}
