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

import com.google.common.base.Preconditions;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import java.util.List;

/**
 * represents window function
 */
public class Window extends Expression implements PropagateNullable {

    private UnboundFunction windowFunction;

    private BoundFunction boundWindowFunction;

    private WindowSpec windowSpec;

    public Window(UnboundFunction windowFunction, WindowSpec windowSpec) {
        super(windowFunction);
//        this.windowFunction = windowFunction;
        this.windowSpec = windowSpec;
    }

    public Window(BoundFunction boundFunction, WindowSpec windowSpec) {
        super(boundFunction);
        this.boundWindowFunction = boundFunction;
        this.windowSpec = windowSpec;
    }

    public Expression getWindowFunction() {
        return child(0);
//        return windowFunction;
    }

    public void setBoundWindowFunction(BoundFunction boundFunction) {
        this.boundWindowFunction = boundFunction;
    }

    public WindowSpec getWindowSpec() {
        return windowSpec;
    }

    @Override
    public String toSql() {
        return getWindowFunction().toSql() + " OVER(" + windowSpec.toSql() + ")";
    }

    @Override
    public String toString() {
        return getWindowFunction() + " " + windowSpec;
    }

    @Override
    public Window withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Window((BoundFunction) children.get(0), windowSpec);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return child(0).getDataType();
    }
}
