using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Duplicati.Library.Utility
{
    /// <summary>
    /// Marks that a parameter should not be disposed of by the caller, it will be handled by the callee.
    /// </summary>
    [System.AttributeUsage(AttributeTargets.Parameter, Inherited = true)]
    public sealed class AssumesOwnershipAttribute: Attribute
    {
    }
}
