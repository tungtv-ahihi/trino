package com.facebook.presto.metadata;

import com.facebook.presto.operator.aggregation.CountFixedWidthAggregation;
import com.facebook.presto.operator.aggregation.DoubleAverageFixedWidthAggregation;
import com.facebook.presto.operator.aggregation.DoubleSumFixedWidthAggregation;
import com.facebook.presto.operator.aggregation.LongAverageFixedWidthAggregation;
import com.facebook.presto.operator.aggregation.LongSumFixedWidthAggregation;
import com.facebook.presto.operator.aggregation.VarBinaryVariableWidthMaxAggregation;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static java.lang.String.format;

public class FunctionRegistry
{
    private final Multimap<QualifiedName, FunctionInfo> functionsByName;
    private final Map<FunctionHandle, FunctionInfo> functionsByHandle;

    public FunctionRegistry()
    {
        List<FunctionInfo> functions = ImmutableList.of(
                new FunctionInfo(1, QualifiedName.of("count"), FIXED_INT_64, ImmutableList.<TupleInfo.Type>of(), FIXED_INT_64, CountFixedWidthAggregation.COUNT),
                new FunctionInfo(2, QualifiedName.of("sum"), FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LongSumFixedWidthAggregation.LONG_SUM),
                new FunctionInfo(3, QualifiedName.of("sum"), DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DoubleSumFixedWidthAggregation.DOUBLE_SUM),
                new FunctionInfo(4, QualifiedName.of("avg"), DOUBLE, ImmutableList.of(DOUBLE), VARIABLE_BINARY, DoubleAverageFixedWidthAggregation.DOUBLE_AVERAGE),
                new FunctionInfo(5, QualifiedName.of("avg"), DOUBLE, ImmutableList.of(FIXED_INT_64), VARIABLE_BINARY, LongAverageFixedWidthAggregation.LONG_AVERAGE),
//                new FunctionInfo(6, QualifiedName.of("max"), FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LongMaxAggregation.BINDER),
//                new FunctionInfo(7, QualifiedName.of("max"), DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DoubleMaxAggregation.BINDER),
                new FunctionInfo(8, QualifiedName.of("max"), VARIABLE_BINARY, ImmutableList.of(VARIABLE_BINARY), VARIABLE_BINARY, VarBinaryVariableWidthMaxAggregation.VAR_BINARY_MAX)
//                new FunctionInfo(9, QualifiedName.of("min"), FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LongMinAggregation.BINDER),
//                new FunctionInfo(10, QualifiedName.of("min"), DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DoubleMinAggregation.BINDER),
//                new FunctionInfo(11, QualifiedName.of("min"), VARIABLE_BINARY, ImmutableList.of(VARIABLE_BINARY), VARIABLE_BINARY, VarBinaryMinAggregation.BINDER)
        );

        functionsByName = Multimaps.index(functions, FunctionInfo.nameGetter());
        functionsByHandle = Maps.uniqueIndex(functions, FunctionInfo.handleGetter());
    }

    public FunctionInfo get(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        for (FunctionInfo functionInfo : functionsByName.get(name)) {
            if (functionInfo.getArgumentTypes().equals(parameterTypes)) {
                return functionInfo;
            }
        }

        throw new IllegalArgumentException(format("Function %s(%s) not registered", name, Joiner.on(", ").join(parameterTypes)));
    }

    public FunctionInfo get(FunctionHandle handle)
    {
        return functionsByHandle.get(handle);
    }
}
