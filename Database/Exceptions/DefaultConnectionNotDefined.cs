using System;

namespace Database.Exceptions
{
  /// <summary>
  /// Exception indicating that an attempt has been made to use an undefined
  ///             default connection.
  /// 
  /// </summary>
  public class DefaultConnectionNotDefined : ApplicationException
  {
    public DefaultConnectionNotDefined(string message)
      : base(message)
    {
    }
  }
}
