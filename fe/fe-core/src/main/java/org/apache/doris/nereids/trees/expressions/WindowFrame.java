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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.functions.window.FrameUnitsType;

import java.util.Optional;

/**
 * window frame
 */
public class WindowFrame {

    private FrameUnitsType frameUnits;

    private FrameBoundary leftBoundary;

    private FrameBoundary rightBoundary;

    public WindowFrame(FrameUnitsType frameUnits, FrameBoundary leftBoundary) {
        this(frameUnits, leftBoundary, null);
    }

    public WindowFrame(FrameUnitsType frameUnits, FrameBoundary leftBoundary, FrameBoundary rightBoundary) {
        this.frameUnits = frameUnits;
        this.leftBoundary = leftBoundary;
        this.rightBoundary = rightBoundary;
    }

    /**
     * if windowFrame's rightBoundary, we should complete it
     */
    public void resolveFrameBoundary(WindowFrame windowFrame) {
        if (windowFrame.rightBoundary != null) {
            return;
        }

        /**
         * "over( rows|range [UNBOUNDED] FOLLOWING)" is invalid.
         */
        if (windowFrame.leftBoundary.getFrameBoundType().isFollowing()) {
            throw new AnalysisException("");
        }

        /**
         * "over( rows|range CURRENT ROW)" equals to "over( row|range between CURRENT ROW and CURRENT ROW)"
         * "over( rows|range [UNBOUNDED] PRECEDING)"
         *      equals to "over( row|range between [UNBOUNDED] PRECEDING and CURRENT ROW)"
         */
        windowFrame.rightBoundary = new FrameBoundary(FrameBoundType.CURRENT_ROW, Optional.empty());
    }

    // confirm that leftBoundary <= rightBoundary
    // check1()

    // confirm that offset of boundary > 0
    // check2()


}
