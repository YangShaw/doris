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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * window spec
 */
public class WindowSpec extends Expression implements PropagateNullable {

    private Optional<List<Expression>> partitionKeyList;

    private Optional<List<OrderKey>> orderKeyList;

    private Optional<WindowFrame> windowFrame;

    public WindowSpec(Optional<List<Expression>> partitionList, Optional<List<OrderKey>> orderKeyList,
                      Optional<WindowFrame> windowFrame) {
        this.partitionKeyList = partitionList;
        this.orderKeyList = orderKeyList;
        this.windowFrame = windowFrame;
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

    public void setWindowFrame(WindowFrame windowFrame) {
        this.windowFrame = Optional.of(windowFrame);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        partitionKeyList.ifPresent(pkList -> sb.append("PARTITION BY ")
                .append(pkList.stream()
                .map(Expression::toSql)
                .collect(Collectors.joining(", ", "", " "))));

        orderKeyList.ifPresent(okList -> sb.append("ORDER BY ")
                .append(okList.stream()
                .map(OrderKey::toSql)
                .collect(Collectors.joining(", ", "", " "))));

        windowFrame.ifPresent(wf -> sb.append(wf.toSql()));
        // if windowFrame is not present, maybe an unused space ", " would be in the end of stringBuilder
        return sb.toString().trim();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("WindowSpec(");

        partitionKeyList.ifPresent(pkList -> sb.append(pkList.stream()
                .map(Expression::toString)
                .collect(Collectors.joining(", ", "", ", "))));

        orderKeyList.ifPresent(okList -> sb.append(okList.stream()
                .map(OrderKey::toString)
                .collect(Collectors.joining(", ", "", ", "))));

        windowFrame.ifPresent(wf -> sb.append(wf));
        String string = sb.toString();
        string = string.endsWith(", ") ? string.substring(0, string.length() - 1) : string;
        return string + ")";
    }
}
