// Copyright © 2026 Robert Schoenstein. All rights reserved.
// Unauthorized use, reproduction, or distribution is strictly prohibited.

using System.Diagnostics.CodeAnalysis;

namespace Database.Dynamic;

public interface IDynamicObject: ICollection<DynamicProperty>
{
    // Interfaces are not serializable
    // The Item property provides methods to read and edit entries
    // in the Dictionary.
    dynamic? this[string name]
    {
        get;
        set;
    }

    // Returns a collections of the keys in this dictionary.
    ICollection<string> Names
    {
        get;
    }

    // Returns a collections of the values in this dictionary.
    ICollection<dynamic?> Values
    {
        get;
    }

    // Returns a collections of the values in this dictionary.
    ICollection<Type> Types
    {
        get;
    }
    
    // Returns whether this collection contains a particular name.
    //
    bool ContainsName(string name);

    // Adds a DynamicProperty to the dictionary.
    //
    void Add(string name, object? value);

    // Adds a DynamicProperty to the dictionary.
    //
    void Add(string name, object? value, Type type);
    
    // Removes a particular name from the collection.
    //
    bool Remove(string name);

    bool TryGetValue(string name, [MaybeNullWhen(false)] out dynamic? value);
}