namespace Database.Dynamic;

public sealed class DynamicProperty
{
    public string Name { get; private set; }
    
    /// <summary>
    /// Get value as object
    /// </summary>
    public object? Value { get; private set; }
    
    public Type Type { get; private set; }
    
    /// <summary>
    /// Convenience constructor that automatically deduces the type.
    /// </summary>
    public DynamicProperty(string name, object? value)
        : this(name, value, value?.GetType() ?? typeof(object))
    { }
    
    public DynamicProperty(string name, object? value, Type type)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(type);

        //Verify type is a primitive type
        if (!(type.IsValueType ||
              type.IsPrimitive ||
              new[]
              {
                  typeof(string),
                  typeof(decimal),
                  typeof(DateTime),
                  typeof(DateTimeOffset),
                  typeof(TimeSpan),
                  typeof(Guid)
              }.Contains(type) ||
              Convert.GetTypeCode(type) != TypeCode.Object))
        {
            throw new ArgumentException($"Type '{type}' is not a supported SQL-convertible type.", nameof(type));
        }
        
        Name = name;
        Value = value;
        Type = type;
    }
    
    public dynamic? GetValue() => Value == null ? null : Convert.ChangeType(Value, Type);
}