// Copyright © 2026 Robert Schoenstein. All rights reserved.
// Unauthorized use, reproduction, or distribution is strictly prohibited.

using System.Diagnostics;
using System.Dynamic;
using System.Linq.Expressions;

namespace Database.Dynamic.Utils;

internal static class StaticUtilities
{
    public static readonly object BoxedFalse = false;
    public static readonly object BoxedTrue = true;
    public static readonly object BoxedIntM1 = -1;
    public static readonly object BoxedInt0 = 0;
    public static readonly object BoxedInt1 = 1;
    public static readonly object BoxedInt2 = 2;
    public static readonly object BoxedInt3 = 3;
    
    private static readonly ConstantExpression _true = Expression.Constant(BoxedTrue);
    private static readonly ConstantExpression _false = Expression.Constant(BoxedFalse);
    private static readonly ConstantExpression s_m1 = Expression.Constant(BoxedIntM1);
    private static readonly ConstantExpression s_0 = Expression.Constant(BoxedInt0);
    private static readonly ConstantExpression s_1 = Expression.Constant(BoxedInt1);
    private static readonly ConstantExpression s_2 = Expression.Constant(BoxedInt2);
    private static readonly ConstantExpression s_3 = Expression.Constant(BoxedInt3);
    
    public static ConstantExpression Constant(bool value) => value ? _true : _false;

    public static ConstantExpression Constant(int value) =>
        value switch
        {
            -1 => s_m1,
            0 => s_0,
            1 => s_1,
            2 => s_2,
            3 => s_3,
            _ => Expression.Constant(value),
        };
    
    public static bool AreEquivalent(Type? t1, Type? t2) => t1 != null && t1.IsEquivalentTo(t2);
    
    /// <summary>
    /// The method takes a DynamicMetaObject, and returns an instance restriction for testing null if the object
    /// holds a null value, otherwise returns a type restriction.
    /// </summary>
    internal static BindingRestrictions GetTypeRestriction(DynamicMetaObject obj)
    {
        Debug.Assert(obj != null);
        if (obj.Value == null && obj.HasValue)
        {
            return BindingRestrictions.GetInstanceRestriction(obj.Expression, null);
        }
        else
        {
            return BindingRestrictions.GetTypeRestriction(obj.Expression, obj.LimitType);
        }
    }
}