package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

public class WindowFunctionChecker extends ExpressionVisitor<Expression, ResolveWindowFunction> {


    @Override
    public Expression visit(Expression expr, ResolveWindowFunction context) {
        return null;
    }

    @Override
    public
}
