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

import com.google.common.collect.ImmutableList;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.expressions.Window;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowSpec;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ResolveWindowFunctionTest extends TestWithFeService implements PatternMatchSupported {

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

    @Test
    public void testRank() {
        String sql = "SELECT rank() over() FROM supplier";
        WindowFrame windowFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(), FrameBoundary.newFollowingBoundary());
        WindowSpec windowSpec = new WindowSpec(Optional.empty(), Optional.empty(),
                Optional.of(windowFrame));

        Window window = new Window(new Rank(), windowSpec);
        Alias alias = new Alias(new ExprId(7), window, "rank() OVER()");
        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
        connectContext.getSessionVariable().setEnableNereidsTrace(true);
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyTopDown(new ExtractWindowExpression())
                .matches(
                    logicalWindow()
                    .when(FieldChecker.check("windowExpressions", ImmutableList.of(alias)))
                );
    }

    @Test
    public void testAnalyze() {
        // String sql = "SELECT sum(s_suppkey) OVER(PARTITION BY s_nation ORDER BY s_name) FROM supplier";
        // String sql2 = "SELECT s_city, sum(s_suppkey) FROM supplier GROUP BY s_city ORDER BY s_city limit 10";
        // String sql2 = "SELECT s_city FROM supplier GROUP BY s_city ORDER BY s_city limit 10";
        // String sql2 = "SELECT s_city FROM supplier ORDER BY s_city limit 10";
        String sql2 = "SELECT s_city FROM supplier ORDER BY s_city limit 10";

        PlanChecker.from(connectContext).checkPlannerResult(sql2);
    }

    @Test
    public void testWindowFunctionChecker() {
        // String rowNumber = "SELECT row_number() over() FROM supplier";

        // String rank = "SELECT rank() over() FROM supplier";
        // 带有参数的window function还没有适配（lag，lead）
        String lag = "SELECT lag(s_suppkey, 1, 2) over() FROM supplier";
        PlanChecker.from(connectContext).checkPlannerResult(lag);
    }

    @Test
    public void testExpr() {
        String substring = "SELECT substring(s_nation, 0, 1+2) FROM supplier";
        PlanChecker.from(connectContext).checkPlannerResult(substring);
    }

    @Test
    public void learnFunctionRegistry() {
        String sql1 = "SELECT sum(s_suppkey) FROM supplier";
        PlanChecker.from(connectContext).checkPlannerResult(sql1);
    }

    @Test
    public void testToString() {
        String sql = "SELECT sum(s_suppkey) OVER(PARTITION BY s_nation ORDER BY s_name ROWS BETWEEN 1 preceding AND current row) FROM supplier";
        PlanChecker.from(connectContext).checkPlannerResult(sql);
    }

    @Test
    public void test() {
        String sql = "SELECT s_suppkey, count(*) FROM supplier GROUP BY s_suppkey ORDER BY s_suppkey";
        PlanChecker.from(connectContext).checkPlannerResult(sql);
    }
}
