// Copyright © 2026 Robert Schoenstein. All rights reserved.
// Unauthorized use, reproduction, or distribution is strictly prohibited.

namespace Database.Dynamic;

/// <summary>
/// Represents a dynamically assigned class.  Expando objects which share the same 
/// members will share the same class.  Classes are dynamically assigned as the
/// expando object gains members.
/// </summary>
internal class DynamicClass
{
    private readonly string[] _names; // list of names associated with each element in the data array, sorted
    private readonly int _hashCode; // pre-calculated hash code of all the names the class contains
    private Dictionary<int, List<WeakReference>> _transitions; // cached transitions

    private const int EmptyHashCode = 6551; // hash code of the empty DynamicClass.

    internal static DynamicClass Empty = new DynamicClass(); // The empty Dynamic class - all Dynamic objects start off w/ this class.

    /// <summary>
    /// Constructs the empty DynamicClass.  This is the class used when an
    /// empty Expando object is initially constructed.
    /// </summary>
    internal DynamicClass()
    {
        _hashCode = EmptyHashCode;
        _names = new string[0];
    }

    /// <summary>
    /// Constructs a new DynamicClass that can hold onto the specified names.  The
    /// names must be sorted ordinally.  The hash code must be precalculated for 
    /// the names.
    /// </summary>
    internal DynamicClass(string[] names, int hashCode)
    {
        _hashCode = hashCode;
        _names = names;
    }

    /// <summary>
    /// Finds or creates a new DynamicClass given the existing set of names
    /// in this DynamicClass plus the new name to be added. Members in an
    /// DynamicClass are always stored case sensitively.
    /// </summary>
    internal DynamicClass FindNewClass(string newName)
    {
        // just XOR the newName hash code 
        int hashCode = _hashCode ^ newName.GetHashCode();

        lock (this)
        {
            List<WeakReference> infos = GetTransitionList(hashCode);

            for (int i = 0; i < infos.Count; i++)
            {
                DynamicClass klass = infos[i].Target as DynamicClass;
                
                if (klass == null)
                {
                    infos.RemoveAt(i);
                    i--;
                    continue;
                }

                if (string.Equals(klass._names[klass._names.Length - 1], newName, StringComparison.Ordinal))
                {
                    // the new key is the key we added in this transition
                    return klass;
                }
            }

            // no applicable transition, create a new one
            string[] names = new string[_names.Length + 1];
            Array.Copy(_names, names, _names.Length);
            names[_names.Length] = newName;
            DynamicClass ec = new DynamicClass(names, hashCode);

            infos.Add(new WeakReference(ec));
            return ec;
        }
    }

    /// <summary>
    /// Gets the lists of transitions that are valid from this DynamicClass
    /// to an DynamicClass whos names hash to the apporopriate hash code.
    /// </summary>
    private List<WeakReference> GetTransitionList(int hashCode)
    {
        if (_transitions == null)
        {
            _transitions = new Dictionary<int, List<WeakReference>>();
        }

        List<WeakReference> infos;
        
        if (!_transitions.TryGetValue(hashCode, out infos))
        {
            _transitions[hashCode] = infos = new List<WeakReference>();
        }

        return infos;
    }

    /// <summary>
    /// Gets the index at which the value should be stored for the specified name.
    /// </summary>
    internal int GetValueIndex(string name, bool caseInsensitive, DynamicObject obj)
    {
        if (caseInsensitive)
        {
            return GetValueIndexCaseInsensitive(name, obj);
        }
        else
        {
            return GetValueIndexCaseSensitive(name);
        }
    }

    /// <summary>
    /// Gets the index at which the value should be stored for the specified name
    /// case sensitively. Returns the index even if the member is marked as deleted.
    /// </summary>
    internal int GetValueIndexCaseSensitive(string name)
    {
        lock (this)
        {
            for (int i = 0; i < _names.Length; i++)
            {
                if (string.Equals(
                        _names[i],
                        name,
                        StringComparison.Ordinal))
                {
                    return i;
                }
            }
        }

        return DynamicObject.NoMatch;
    }

    /// <summary>
    /// Gets the index at which the value should be stored for the specified name,
    /// the method is only used in the case-insensitive case.
    /// </summary>
    /// <param name="name">the name of the member</param>
    /// <param name="obj">The ExpandoObject associated with the class
    /// that is used to check if a member has been deleted.</param>
    /// <returns>
    /// the exact match if there is one
    /// if there is exactly one member with case insensitive match, return it
    /// otherwise we throw AmbiguousMatchException.
    /// </returns>
    private int GetValueIndexCaseInsensitive(string name, DynamicObject obj)
    {
        int caseInsensitiveMatch = DynamicObject.NoMatch; //the location of the case-insensitive matching member
        lock (obj.LockObject)
        {
            for (int i = _names.Length - 1; i >= 0; i--)
            {
                if (string.Equals(
                        _names[i],
                        name,
                        StringComparison.OrdinalIgnoreCase))
                {
                    //if the matching member is deleted, continue searching
                    if (!obj.IsDeletedMember(i))
                    {
                        if (caseInsensitiveMatch == DynamicObject.NoMatch)
                        {
                            caseInsensitiveMatch = i;
                        }
                        else
                        {
                            //Ambigous match, stop searching
                            return DynamicObject.AmbiguousMatchFound;
                        }
                    }
                }
            }
        }

        //There is exactly one member with case insensitive match.
        return caseInsensitiveMatch;
    }

    /// <summary>
    /// Gets the names of the names that can be stored in the Expando class.  The
    /// list is sorted ordinally.
    /// </summary>
    internal string[] Names
    {
        get { return _names; }
    }
}