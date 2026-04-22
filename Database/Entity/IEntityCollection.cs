// Copyright © 2026 Robert Schoenstein.
// WARNING: Do not use this software or code without the written consent of the author.

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;

namespace Database.Entity;

public interface IEntityCollection<T> : IList<T>, IListSource
{
    int RemoveAll(Predicate<T> match);

    void AddRange(IEnumerable<T> range);
}