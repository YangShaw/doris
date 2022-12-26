package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.trees.expressions.NamedExpression;

import java.util.List;

public interface Window {

    List<NamedExpression> getOutputExpressions();
}
