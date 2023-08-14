using System;
using System.Reflection;

namespace Database
{
  [Serializable]
  public class TypeConversionException : Exception
  {
    private string _propertyName;
    private Type _propertyType;

    public string PropertyName
    {
      get { return _propertyName; ; }
      set { _propertyName = value; }
    }

    public Type PropertyType
    {
      get { return _propertyType; }
      set { _propertyType = value; }
    }

    public TypeConversionException()
    { }

    public TypeConversionException(PropertyInfo propertyInfo, string message, Exception innerException)
      : base(message, innerException)
    {
      _propertyName = propertyInfo.Name;
      _propertyType = propertyInfo.PropertyType;
    }
  }
}
