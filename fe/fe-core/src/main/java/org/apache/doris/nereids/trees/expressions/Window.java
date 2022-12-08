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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * represents window function. WindowFunction of this window is saved as Window's child,
 * which is an UnboundFunction at first and will be analyzed as relevant BoundFunction
 * (may a WindowFunction or AggregateFunction) after BindFunction.
 */
public class Window extends Expression implements UnaryExpression, PropagateNullable {

    private WindowSpec windowSpec;

    /** for test only*/
    public Window(Expression windowFunction) {
        this(windowFunction, new WindowSpec());
    }

    public Window(Expression windowFunction, WindowSpec windowSpec) {
        super(windowFunction);
        this.windowSpec = windowSpec;
    }

    public Expression getWindowFunction() {
        return child();
    }

    public WindowSpec getWindowSpec() {
        return windowSpec;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Window that = (Window) o;
        return Objects.equals(windowSpec, that.windowSpec)
            && Objects.equals(getWindowFunction(), that.getWindowFunction());
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowSpec, getWindowFunction());
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
        return new Window(children.get(0), windowSpec);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return child().getDataType();
    }
}
