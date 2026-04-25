// Copyright © 2026 Robert Schoenstein. All rights reserved.
// Unauthorized use, reproduction, or distribution is strictly prohibited.

namespace Database.Dynamic;

/// <summary>
/// Represents a dynamically assigned class.  Dynamic objects which share the same 
/// members will share the same class.  Classes are dynamically assigned as the
/// dynamic object gains members.
/// </summary>
internal class FlexClass
{
    // list of names associated with each element in the data array, sorted
    private readonly string[] _names;
    // pre-calculated hash code of all the names the class contains
    private readonly int _hashCode;
    // cached transitions
    private Dictionary<int, List<WeakReference>> _transitions;

    private const int EmptyHashCode = 6551; // hash code of the empty FlexClass.

    internal static FlexClass Empty = new FlexClass(); // The empty Dynamic class - all Dynamic objects start off w/ this class.

    /// <summary>
    /// Constructs the empty FlexClass.  This is the class used when an
    /// empty Expando object is initially constructed.
    /// </summary>
    internal FlexClass()
    {
        _hashCode = EmptyHashCode;
        _names = Array.Empty<string>();
    }

    /// <summary>
    /// Constructs a new FlexClass that can hold onto the specified names.  The
    /// names must be sorted ordinally.  The hash code must be precalculated for 
    /// the names.
    /// </summary>
    internal FlexClass(string[] names, int hashCode)
    {
        _hashCode = hashCode;
        _names = names;
    }

    /// <summary>
    /// Finds or creates a new FlexClass given the existing set of names
    /// in this FlexClass plus the new name to be added. Members in an
    /// FlexClass are always stored case sensitively.
    /// </summary>
    internal FlexClass FindNewClass(string newName)
    {
        // just XOR the newName hash code 
        int hashCode = _hashCode ^ newName.GetHashCode();

        lock (this)
        {
            _transitions ??= new Dictionary<int, List<WeakReference>>();

            if (!_transitions.TryGetValue(hashCode, out var list))
            {
                list = new List<WeakReference>();
                _transitions[hashCode] = list;
            }

            // Clean dead references and check for existing transition
            for (int i = 0; i < list.Count; i++)
            {
                if (list[i].Target is FlexClass klass)
                {
                    if (string.Equals(klass._names[^1], newName, StringComparison.Ordinal))
                    {
                        return klass;
                    }
                }
                else
                {
                    list.RemoveAt(i--);
                }
            }

            // Create new class
            string[] newNames = new string[_names.Length + 1];
            Array.Copy(_names, newNames, _names.Length);
            newNames[^1] = newName;

            var newClass = new FlexClass(newNames, hashCode);
            list.Add(new WeakReference(newClass));
            return newClass;
        }
    }

    /// <summary>
    /// Gets the index at which the value should be stored for the specified name
    /// case sensitively. Returns the index even if the member is marked as deleted.
    /// </summary>
    internal int GetValueIndexCaseSensitive(string name)
    {
        for (int i = 0; i < _names.Length; i++)
        {
            if (string.Equals(_names[i], name, StringComparison.Ordinal))
                return i;
        }
        return FlexObject.NoMatch;
    }

    /// <summary>
    /// Gets the index at which the value should be stored for the specified name,
    /// the method is only used in the case-insensitive case.
    /// </summary>
    /// <param name="name">the name of the member</param>
    /// <param name="ignoreCase">ignore case of member name</param>
    /// <param name="obj">The ExpandoObject associated with the class
    /// that is used to check if a member has been deleted.</param>
    /// <returns>
    /// the exact match if there is one
    /// if there is exactly one member with case insensitive match, return it
    /// otherwise we throw AmbiguousMatchException.
    /// </returns>
    internal int GetValueIndex(string name, bool ignoreCase, FlexObject obj)
    {
        if (!ignoreCase)
        {
            return GetValueIndexCaseSensitive(name);
        }

        int match = FlexObject.NoMatch;
        for (int i = _names.Length - 1; i >= 0; i--)
        {
            if (string.Equals(_names[i], name, StringComparison.OrdinalIgnoreCase))
            {
                if (!obj.IsDeletedMember(i))
                {
                    if (match == FlexObject.NoMatch)
                        match = i;
                    else
                        return FlexObject.AmbiguousMatchFound;
                }
            }
        }
        return match;
    }
}