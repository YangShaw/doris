package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;

public abstract class FirstOrLastValue extends WindowFunction implements UnaryExpression {

    public FirstOrLastValue(String name, Expression child) {
        super(name, child);
    }

    public FirstOrLastValue reverse() {
        if (this instanceof FirstValue) {
            return new LastValue(child());
        } else {
            return new FirstValue(child());
        }
    }
}
