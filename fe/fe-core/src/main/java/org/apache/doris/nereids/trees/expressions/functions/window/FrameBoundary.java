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

import java.util.Objects;
import java.util.Optional;

/**
 * frame boundary
 */
public class FrameBoundary {

    private Optional<Expression> boundOffset;
    private FrameBoundType frameBoundType;

    public FrameBoundary(FrameBoundType frameBoundType) {
        this.boundOffset = Optional.empty();
        this.frameBoundType = frameBoundType;
    }

    public FrameBoundary(Optional<Expression> boundOffset, FrameBoundType frameBoundType) {
        this.boundOffset = boundOffset;
        this.frameBoundType = frameBoundType;
    }

    public static FrameBoundary newPrecedingBoundary() {
        return new FrameBoundary(FrameBoundType.UNBOUNDED_PRECEDING);
    }

    public static FrameBoundary newPrecedingBoundary(Expression boundValue) {
        return new FrameBoundary(Optional.of(boundValue), FrameBoundType.UNBOUNDED_PRECEDING);
    }

    public static FrameBoundary newFollowingBoundary() {
        return new FrameBoundary(FrameBoundType.UNBOUNDED_FOLLOWING);
    }

    public static FrameBoundary newFollowingBoundary(Expression boundValue) {
        return new FrameBoundary(Optional.of(boundValue), FrameBoundType.UNBOUNDED_FOLLOWING);
    }

    public static FrameBoundary newCurrentRowBoundary() {
        return new FrameBoundary(FrameBoundType.CURRENT_ROW);
    }

    public boolean is(FrameBoundType otherType) {
        return this.frameBoundType == otherType;
    }

    public boolean isNot(FrameBoundType otherType) {
        return this.frameBoundType != otherType;
    }

    public boolean isNull() {
        return this.frameBoundType == FrameBoundType.EMPTY_BOUNDARY;
    }

    public boolean hasOffset() {
        return frameBoundType == FrameBoundType.PRECEDING || frameBoundType == FrameBoundType.FOLLOWING;
    }

    public boolean asPreceding() {
        return frameBoundType == FrameBoundType.PRECEDING || frameBoundType == FrameBoundType.UNBOUNDED_PRECEDING;
    }

    public boolean asFollowing() {
        return frameBoundType == FrameBoundType.FOLLOWING || frameBoundType == FrameBoundType.UNBOUNDED_FOLLOWING;
    }

    public FrameBoundary reverse() {
        return new FrameBoundary(boundOffset, frameBoundType.reverse());
    }

    public FrameBoundType getFrameBoundType() {
        return frameBoundType;
    }

    public void setFrameBoundType(FrameBoundType frameBoundType) {
        this.frameBoundType = frameBoundType;
    }

    public Optional<Expression> getBoundOffset() {
        return boundOffset;
    }

    public void setBoundOffset(Optional<Expression> boundOffset) {
        this.boundOffset = boundOffset;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boundOffset.ifPresent(value -> sb.append(value + " "));
        sb.append(frameBoundType);

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FrameBoundary other = (FrameBoundary) o;
        return Objects.equals(this.frameBoundType, other.frameBoundType)
            && Objects.equals(this.boundOffset, other.boundOffset);
    }
}
