using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Database.Entity
{
  [AttributeUsage(AttributeTargets.Property)]
  public class TVPIgnore : Attribute
  {
  }
}
