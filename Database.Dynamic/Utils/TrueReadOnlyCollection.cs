// Copyright © 2026 Robert Schoenstein. All rights reserved.
// Unauthorized use, reproduction, or distribution is strictly prohibited.

using System.Collections.ObjectModel;

namespace Database.Dynamic.Utils;

internal sealed class TrueReadOnlyCollection<T> : ReadOnlyCollection<T>
{
    /// <summary>
    /// Creates instance of TrueReadOnlyCollection, wrapping passed in array.
    /// !!! DOES NOT COPY THE ARRAY !!!
    /// </summary>
    public TrueReadOnlyCollection(params T[] list)
        : base(list)
    {
    }
}