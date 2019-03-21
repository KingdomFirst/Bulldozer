﻿// <copyright>
// Copyright 2019 by Kingdom First Solutions
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>
//
using System;
using Rock.Model;

namespace Bulldozer.Utility
{
    public static partial class Extensions
    {
        // Flag to designate household role
        public enum FamilyRole
        {
            Adult = 0,
            Child = 1,
            Visitor = 2
        };

        public enum SearchDirection
        {
            Begins = 0,
            Ends = 1
        };

        /// <summary>
        /// Helper class to store references to people that've been imported
        /// </summary>
        public class PersonKeys
        {
            /// <summary>
            /// Stores the Rock PersonAliasId
            /// </summary>
            public int PersonAliasId;

            /// <summary>
            /// Stores the Rock PersonId
            /// </summary>
            public int PersonId;

            /// <summary>
            /// Stores the Rock Person Gender
            /// </summary>
            public Gender PersonGender;

            /// <summary>
            /// Stores a Person's Foreign Id
            /// </summary>
            public int? PersonForeignId;

            /// <summary>
            /// Stores a Person's Foreign Key
            /// </summary>
            public string PersonForeignKey;

            /// <summary>
            /// Stores a Group's Foreign Id
            /// </summary>
            public int? GroupForeignId;

            /// <summary>
            /// Stores how the person is connected to the family
            /// </summary>
            public FamilyRole FamilyRoleId;
        }

        /// <summary>
        /// Helper class to store document keys
        /// </summary>
        public class DocumentKeys
        {
            /// <summary>
            /// Stores the Rock PersonId
            /// </summary>
            public int PersonId;

            /// <summary>
            /// Stores the attribute linked to this document
            /// </summary>
            public int AttributeId;

            /// <summary>
            /// Stores the actual document
            /// </summary>
            public BinaryFile File;
        }

        /// <summary>
        /// Helper class to handle attendance occurrences
        /// </summary>
        public class ImportOccurrence
        {
            public int Id { get; set; }
            public int? GroupId { get; set; }
            public int? LocationId { get; set; }
            public int? ScheduleId { get; set; }
            public DateTime OccurrenceDate { get; set; }
        }
    }
}