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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * represents window function. WindowFunction of this window is saved as Window's child,
 * which is an UnboundFunction at first and will be analyzed as relevant BoundFunction
 * (can be a WindowFunction or AggregateFunction) after BindFunction.
 */
public class Window extends Expression implements PropagateNullable {

    private final Expression function;

    private final List<Expression> partitionKeyList;

    private final List<OrderExpression> orderKeyList;

    private final Optional<WindowFrame> windowFrame;

    /** constructor of Window*/
    public Window(Expression function, List<Expression> partitionKeyList, List<OrderExpression> orderKeyList) {
        super(new ImmutableList.Builder<Expression>()
                .add(function)
                .addAll(partitionKeyList)
                .addAll(orderKeyList)
                .build().toArray(new Expression[0]));
        this.function = function;
        this.partitionKeyList = ImmutableList.copyOf(partitionKeyList);
        this.orderKeyList = ImmutableList.copyOf(orderKeyList);
        this.windowFrame = Optional.empty();
    }

    /** constructor of Window*/
    public Window(Expression function, List<Expression> partitionKeyList, List<OrderExpression> orderKeyList,
                  WindowFrame windowFrame) {
        super(new ImmutableList.Builder<Expression>()
                .add(function)
                .addAll(partitionKeyList)
                .addAll(orderKeyList)
                .add(windowFrame)
                .build().toArray(new Expression[0]));
        this.function = function;
        this.partitionKeyList = ImmutableList.copyOf(partitionKeyList);
        this.orderKeyList = ImmutableList.copyOf(orderKeyList);
        this.windowFrame = Optional.of(Objects.requireNonNull(windowFrame));
    }

    public Expression getFunction() {
        return function;
    }

    /**
     * extract expressions from function, partitionKeys and orderKeys
     * todo: expressions from WindowFrame
     */
    public List<Expression> getExpressionsInWindowSpec() {
        List<Expression> expressions = Lists.newArrayList();
        expressions.addAll(function.children());
        expressions.addAll(partitionKeyList);
        expressions.addAll(orderKeyList.stream()
                .map(orderExpression -> orderExpression.child())
                .collect(Collectors.toList()));
        return expressions;
    }

    public List<Expression> getPartitionKeyList() {
        return partitionKeyList;
    }

    public List<OrderExpression> getOrderKeyList() {
        return orderKeyList;
    }

    public Optional<WindowFrame> getWindowFrame() {
        return windowFrame;
    }

    public Window withWindowFrame(WindowFrame windowFrame) {
        return new Window(function, partitionKeyList, orderKeyList, windowFrame);
    }

    public Window withOrderKeyList(List<OrderExpression> orderKeyList) {
        if (windowFrame.isPresent()) {
            return new Window(function, partitionKeyList, orderKeyList, windowFrame.get());
        }
        return new Window(function, partitionKeyList, orderKeyList);
    }

    @Override
    public Window withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1);
        int index = 0;
        Expression func = children.get(index);
        index += 1;

        List<Expression> partitionKeys = children.subList(index, index + partitionKeyList.size());
        index += partitionKeyList.size();

        List<OrderExpression> orderKeys = children.subList(index, index + orderKeyList.size()).stream()
                .map(OrderExpression.class::cast)
                .collect(Collectors.toList());
        index += orderKeyList.size();

        if (index < children.size()) {
            return new Window(func, partitionKeys, orderKeys, (WindowFrame) children.get(index));
        }
        return new Window(func, partitionKeys, orderKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Window window = (Window) o;
        return Objects.equals(function, window.function)
            && Objects.equals(partitionKeyList, window.partitionKeyList)
            && Objects.equals(orderKeyList, window.orderKeyList)
            && Objects.equals(windowFrame, window.windowFrame);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, partitionKeyList, orderKeyList, windowFrame);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(function.toSql() + " OVER(");
        if (!partitionKeyList.isEmpty()) {
            sb.append("PARTITION BY ").append(partitionKeyList.stream()
                    .map(Expression::toSql)
                    .collect(Collectors.joining(", ", "", " ")));
        }
        if (!orderKeyList.isEmpty()) {
            sb.append("ORDER BY ").append(orderKeyList.stream()
                    .map(OrderExpression::toSql)
                    .collect(Collectors.joining(", ", "", " ")));
        }
        windowFrame.ifPresent(wf -> sb.append(wf.toSql()));
        sb.append(")");
        return sb.toString().trim();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(function + " WindowSpec(");
        if (!partitionKeyList.isEmpty()) {
            sb.append("PARTITION BY ").append(partitionKeyList.stream()
                    .map(Expression::toString)
                    .collect(Collectors.joining(", ", "", " ")));
        }
        if (!orderKeyList.isEmpty()) {
            sb.append("ORDER BY ").append(orderKeyList.stream()
                    .map(OrderExpression::toString)
                    .collect(Collectors.joining(", ", "", " ")));
        }
        windowFrame.ifPresent(wf -> sb.append(wf.toSql()));
        String string = sb.toString();
        // if windowFrame is not present, maybe an unused string ", " would be in the end of stringBuilder
        string = string.endsWith(", ") ? string.substring(0, string.length() - 2) : string;
        return string + ")";
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWindow(this, context);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return function.getDataType();
    }
}
