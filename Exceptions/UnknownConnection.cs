using System;

namespace Database.Exceptions
{
  /// <summary>
  /// Exception indicating that an attempt has been made to use an undefined
  ///             connection.
  /// 
  /// </summary>
  public class UnknownConnection : ApplicationException
  {
    public UnknownConnection(string message)
      : base(message)
    {
    }
  }
}
