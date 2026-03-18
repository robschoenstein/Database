using System;

namespace Database.Exceptions;

public class DefaultConnectionRemoval : ApplicationException
{
    public DefaultConnectionRemoval() : base("Unable to remove default connection. Try updating it instead.")
    { }
}