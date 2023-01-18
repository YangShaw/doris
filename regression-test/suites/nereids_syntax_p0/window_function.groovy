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

suite("test_window_function") {
    sql "SET enable_nereids_planner=true"

    sql "DROP TABLE IF EXISTS window_test"

    sql """
        CREATE TABLE `window_test` (
            `c1` int NULL,
            `c2` int NULL,
            `c3` double NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`c1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """INSERT INTO window_test VALUES(1, 1, 1)"""
    sql """INSERT INTO window_test VALUES(1, 2, 1)"""
    sql """INSERT INTO window_test VALUES(1, 3, 1)"""
    sql """INSERT INTO window_test VALUES(2, 1, 1)"""
    sql """INSERT INTO window_test VALUES(2, 2, 1)"""
    sql """INSERT INTO window_test VALUES(2, 3, 1)"""
    sql """INSERT INTO window_test VALUES(1, 1, 2)"""
    sql """INSERT INTO window_test VALUES(1, 2, 2)"""
    sql """INSERT INTO window_test VALUES(2, 1, 2)"""
    sql """INSERT INTO window_test VALUES(2, 2, 2)"""

    sql "SET enable_fallback_to_original_planner=false"

    order_qt_select "SELECT rank() over(partition by col3 order by col2) FROM window_test"
    order_qt_select "SELECT dense_rank() over(partition by col3 order by col2) FROM window_test"
    order_qt_select "SELECT row_number() over(partition by col3 order by col2) FROM window_test"
    order_qt_select "SELECT sum(col1) over(partition by col3 order by col2) FROM window_test"
    order_qt_select "SELECT avg(col1) over(partition by col3 order by col2) FROM window_test"
    order_qt_select "SELECT count(col1) over(partition by col3 order by col2) FROM window_test"
}
