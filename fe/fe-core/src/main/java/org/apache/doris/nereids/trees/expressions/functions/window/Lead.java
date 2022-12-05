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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import java.util.List;

/**
 * Window function: Lead()
 */
public class Lead extends WindowFunction implements TernaryExpression {

    public Lead(Expression child) {
        this(child, Literal.of(1), Literal.of(null));
    }

    public Lead(Expression child, Expression offset) {
        this(child, offset, Literal.of(null));
    }

    public Lead(Expression child, Expression offset, Expression defaultValue) {
        super("lead", child, offset, defaultValue);
    }

    public Expression getOffset() {
        return child(1);
    }

    public Expression getDefaultValue() {
        return child(2);
    }

    public void setDefaultValue(Expression defaultValue) {
        this.children.set(2, defaultValue);
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLead(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return null;
    }

    @Override
    public FunctionSignature searchSignature(List<DataType> argumentTypes, List<Expression> arguments,
                                             List<FunctionSignature> signatures) {
        return null;
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }
}
