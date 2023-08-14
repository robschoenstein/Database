using System;

namespace Database
{
  [Serializable]
  public class PopulationException : Exception
  {
    private string _propertyName;
    private Type _propertyType;
    private string _columnName;
    private Type _columnType;
    private object _value;

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

    public string ColumnName
    {
      get { return _columnName; }
      set { _columnName = value; }
    }

    public Type ColumnType
    {
      get { return _columnType; }
      set { _columnType = value; }
    }

    public object Value
    {
      get { return _value; }
      set { _value = value; }
    }

    public PopulationException()
    { }

    public PopulationException(string propertyName, Type propertyType, string columnName, Type columnType, object value, string message, Exception ex)
      : base(message, ex)
    {
      _propertyName = propertyName;
      _propertyType = propertyType;
      _columnName = columnName;
      _columnType = columnType;
      _value = value;
    }
  }
}
