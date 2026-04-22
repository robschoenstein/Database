using System;

namespace Database.Exceptions;

public class DataNotInitialized : Exception
{
    public DataNotInitialized(string message) : base(message)
    { }
}