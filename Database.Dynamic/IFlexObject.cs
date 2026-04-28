using System.Diagnostics.CodeAnalysis;

namespace Database.Dynamic;

public interface IFlexObject: ICollection<FlexProperty>
{
    // Interfaces are not serializable
    // The Item property provides methods to read and edit entries
    // in the Dictionary.
    object? this[string name] { get; set; }

    // Returns a collection of the keys in this dictionary.
    ICollection<string> Names { get; }

    // Returns a collection of the values in this dictionary.
    ICollection<object?> Values { get; }

    // Returns a collection of the values in this dictionary.
    ICollection<Type> Types { get; }
    
    // Returns whether this collection contains a particular name.
    //
    bool ContainsName(string name);

    void Add(string name, object? value);

    void Add(string name, object? value, Type type);
    
    // Removes a particular name from the collection.
    //
    bool Remove(string name);

    bool TryGetValue(string name, [MaybeNullWhen(false)] out object? value);
}