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

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;

/**
 * represents window function
 */
public class Window extends Expression implements PropagateNullable {

    private UnboundFunction windowFunction;

    private WindowSpec windowSpec;

    public Window(UnboundFunction windowFunction, WindowSpec windowSpec) {
        this.windowFunction = windowFunction;
        this.windowSpec = windowSpec;
    }

    public UnboundFunction getWindowFunction() {
        return windowFunction;
    }

    public WindowSpec getWindowSpec() {
        return windowSpec;
    }

    @Override
    public String toSql() {
        return windowFunction.toSql() + " OVER(" + windowSpec.toSql() + ")";
    }

    @Override
    public String toString() {
        return windowFunction + " " + windowSpec;
    }
}
