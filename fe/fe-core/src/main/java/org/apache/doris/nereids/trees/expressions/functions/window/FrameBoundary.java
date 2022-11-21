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

import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.Optional;

/**
 * frame boundary
 */
public class FrameBoundary {

    private FrameBoundType frameBoundType;

    private Optional<Expression> boundValue;

    public FrameBoundary(FrameBoundType frameBoundType) {
        this.frameBoundType = frameBoundType;
        this.boundValue = Optional.empty();
    }

    public FrameBoundary(FrameBoundType frameBoundType, Optional<Expression> boundValue) {
        this.frameBoundType = frameBoundType;
        this.boundValue = boundValue;
    }

    public static FrameBoundary newPrecedingBoundary() {
        return new FrameBoundary(FrameBoundType.UNBOUNDED_PRECEDING);
    }

    public static FrameBoundary newFollowingBoundary() {
        return new FrameBoundary(FrameBoundType.UNBOUNDED_FOLLOWING);
    }

    public static FrameBoundary newCurrentRowBoundary() {
        return new FrameBoundary(FrameBoundType.CURRENT_ROW);
    }

    public FrameBoundType getFrameBoundType() {
        return frameBoundType;
    }

    public void setFrameBoundType(FrameBoundType frameBoundType) {
        this.frameBoundType = frameBoundType;
    }

    public Optional<Expression> getBoundValue() {
        return boundValue;
    }

    public void setBoundValue(Optional<Expression> boundValue) {
        this.boundValue = boundValue;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boundValue.ifPresent(value -> sb.append(value + " "));
        sb.append(frameBoundType);

        return sb.toString();
    }
}
