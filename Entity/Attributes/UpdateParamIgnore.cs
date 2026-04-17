// Copyright © 2026 Robert Schoenstein. All rights reserved.
// Unauthorized use, reproduction, or distribution is strictly prohibited.

using System;

namespace Database.Entity.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public class UpdateParamIgnore : Attribute
{ }