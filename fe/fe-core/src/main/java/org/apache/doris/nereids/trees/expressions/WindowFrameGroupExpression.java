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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.List;
import java.util.Objects;

/**
 * an encapsulation of WindowFrameGroup; just for ExpressionTranslator
 */
public class WindowFrameGroupExpression extends Expression implements PropagateNullable, LeafExpression {

    private List<Expression> partitionKeyList;
    private List<OrderKey> orderKeyList;
    private WindowFrame windowFrame;
    private List<NamedExpression> windowFunctionList;

    public WindowFrameGroupExpression(WindowFrameGroup windowFrameGroup) {
        partitionKeyList = windowFrameGroup.getPartitionKeyList();
        orderKeyList = windowFrameGroup.getOrderKeyList();
        windowFrame = windowFrameGroup.getWindowFrame();
        windowFunctionList = windowFrameGroup.getGroupList();
    }

    public List<Expression> getPartitionKeyList() {
        return partitionKeyList;
    }

    public List<OrderKey> getOrderKeyList() {
        return orderKeyList;
    }

    public WindowFrame getWindowFrame() {
        return windowFrame;
    }

    public List<NamedExpression> getWindowFunctionList() {
        return windowFunctionList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowFrameGroupExpression that = (WindowFrameGroupExpression) o;
        return Objects.equals(partitionKeyList, that.partitionKeyList)
            && Objects.equals(orderKeyList, that.orderKeyList)
            && Objects.equals(windowFrame, that.windowFrame)
            && Objects.equals(windowFunctionList, that.windowFunctionList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionKeyList, orderKeyList, windowFrame, windowFunctionList);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWindowFrameGroupExpression(this, context);
    }
}
