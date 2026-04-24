// Copyright © 2026 Robert Schoenstein. All rights reserved.
// Unauthorized use, reproduction, or distribution is strictly prohibited.

namespace Database.Dynamic.Exceptions;

public class NameNotFoundException : SystemException
{
    public NameNotFoundException(string? name)
        : base($"Name not found: {name}")
    {}

    public NameNotFoundException(string? message, string? name)
        : base(message ?? $"Name not found: {name}")
    { }

    public NameNotFoundException(string? message, string? name, Exception? innerException)
        : base(message ?? $"Name not found: {name}", innerException)
    { }
}