using System;

namespace Database.Entity
{
  [AttributeUsage(AttributeTargets.Field)]
  public class StringValueAttribute : Attribute
  {
    public string StringValue { get; set; }

    public StringValueAttribute(string value)
    {
      StringValue = value;
    }
  }
}
