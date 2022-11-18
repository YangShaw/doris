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

import java.util.List;
import java.util.Optional;

/**
 * window spec
 */
public class WindowSpec {

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
}
