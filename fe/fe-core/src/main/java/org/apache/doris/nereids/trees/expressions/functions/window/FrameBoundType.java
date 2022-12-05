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

/**
 * frame bound types
 */
public enum FrameBoundType {

    UNBOUNDED_PRECEDING("UNBOUNDED_PRECEDING"),
    UNBOUNDED_FOLLOWING("UNBOUNDED_FOLLOWING"),
    CURRENT_ROW("CURRENT_ROW"),
    PRECEDING("PRECEDING"),
    FOLLOWING("FOLLOWING"),

    // represents that the boundary is null. We use this value as default
    // to avoid checking if a boundary is null frequently.
    EMPTY_BOUNDARY("EMPTY_BOUNDARY");

    private final String description;

    FrameBoundType(String description) {
        this.description = description;
    }

    /**
     * reverse current FrameBoundType
     */
    public FrameBoundType reverse() {
        switch (this) {
            case UNBOUNDED_PRECEDING:
                return UNBOUNDED_FOLLOWING;
            case UNBOUNDED_FOLLOWING:
                return UNBOUNDED_PRECEDING;
            case PRECEDING:
                return FOLLOWING;
            case FOLLOWING:
                return PRECEDING;
            case CURRENT_ROW:
                return CURRENT_ROW;
            default:
                return EMPTY_BOUNDARY;
        }
    }

    public boolean isFollowing() {
        return this.equals(UNBOUNDED_FOLLOWING) || this.equals(FOLLOWING);
    }

}
