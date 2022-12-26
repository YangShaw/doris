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

import com.google.common.base.Preconditions;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Window function: Rank()
 */
public class Rank extends WindowFunction implements AlwaysNotNullable, LeafExpression {

    public Rank() {
        super("rank");
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return null;
    }

    @Override
    public FunctionSignature searchSignature(List<DataType> argumentTypes,
                                             List<Expression> arguments, List<FunctionSignature> signatures) {
        return null;
    }

    @Override
    protected FunctionSignature computeSignature(FunctionSignature signature, List<Expression> arguments) {
        return null;
    }

    @Override
    public Rank withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 0);
        return new Rank();
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRank(this, context);
    }

    @Override
    public DataType getDataType() {
        return IntegerType.INSTANCE;
    }
}
