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
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
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
public class Window extends Expression implements UnaryExpression, PropagateNullable {

    private Optional<List<Expression>> partitionKeyList;

    private Optional<List<OrderKey>> orderKeyList;

    private Optional<WindowFrame> windowFrame;

    /** for test only*/
    public Window(Expression function) {
        this(function, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public Window(Expression function, Optional<List<Expression>> partitionKeyList,
                  Optional<List<OrderKey>> orderKeyList, Optional<WindowFrame> windowFrame) {
        super(function);
        this.partitionKeyList = partitionKeyList;
        this.orderKeyList = orderKeyList;
        this.windowFrame = windowFrame;
    }

    public Expression getWindowFunction() {
        return child();
    }

    /**
     * extract expressions from function, partitionKeys and orderKeys
     * todo: expressions from WindowFrame
     */
    public List<Expression> getExpressionsInWindowSpec() {
        List<Expression> expressions = Lists.newArrayList();
        expressions.addAll(child().children());
        partitionKeyList.ifPresent(list -> expressions.addAll(list));
        orderKeyList.ifPresent(list -> expressions.addAll(list.stream()
                .map(orderKey -> orderKey.getExpr())
                .collect(Collectors.toList()))
        );
        return expressions;
    }

    public Optional<List<Expression>> getPartitionKeyList() {
        return partitionKeyList;
    }

    public Optional<List<OrderKey>> getOrderKeyList() {
        return orderKeyList;
    }

    public Optional<WindowFrame> getWindowFrame() {
        return windowFrame;
    }

    public Window withWindowFrame(WindowFrame windowFrame) {
        return new Window(child(), partitionKeyList, orderKeyList, Optional.of(windowFrame));
    }

    public Window withOrderKeyList(List<OrderKey> orderKeyList) {
        return new Window(child(), partitionKeyList, Optional.of(orderKeyList), windowFrame);
    }

    @Override
    public Window withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Window(children.get(0), partitionKeyList, orderKeyList, windowFrame);
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
        return Objects.equals(child(), window.child())
            && Objects.equals(partitionKeyList, window.partitionKeyList)
            && Objects.equals(orderKeyList, window.orderKeyList)
            && Objects.equals(windowFrame, window.windowFrame);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), partitionKeyList, orderKeyList, windowFrame);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(getWindowFunction().toSql() + " OVER(");
        partitionKeyList.ifPresent(pkList -> sb.append("PARTITION BY ")
                .append(pkList.stream()
                .map(Expression::toSql)
                .collect(Collectors.joining(", ", "", " "))));

        orderKeyList.ifPresent(okList -> sb.append("ORDER BY ")
                .append(okList.stream()
                .map(OrderKey::toSql)
                .collect(Collectors.joining(", ", "", " "))));

        windowFrame.ifPresent(wf -> sb.append(wf.toSql()));
        sb.append(")");
        return sb.toString().trim();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getWindowFunction() + " WindowSpec(");

        partitionKeyList.ifPresent(pkList -> sb.append("PARTITION BY ")
                .append(pkList.stream()
                .map(Expression::toString)
                .collect(Collectors.joining(", ", "", ", "))));

        orderKeyList.ifPresent(okList -> sb.append("ORDER BY ")
                .append(okList.stream()
                .map(OrderKey::toString)
                .collect(Collectors.joining(", ", "", ", "))));

        windowFrame.ifPresent(wf -> sb.append(wf));
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
        return child().getDataType();
    }
}
