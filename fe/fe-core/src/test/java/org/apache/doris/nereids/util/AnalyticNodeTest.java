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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class AnalyticNodeTest extends TestWithFeService {

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

    @Test
    public void testAnalyze() {
        String sql = "SELECT sum(s_suppkey) OVER(PARTITION BY s_nation ORDER BY s_name) FROM supplier";
        PlanChecker.from(connectContext).checkPlannerResult(sql);
    }

    @Test
    public void test() {
        String sql = "SELECT s_suppkey, count(*) FROM supplier GROUP BY s_suppkey ORDER BY s_suppkey";
        PlanChecker.from(connectContext).checkPlannerResult(sql);
    }
}
