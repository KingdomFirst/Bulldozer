﻿// <copyright>
// Copyright 2022 by Kingdom First Solutions
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
using System.Collections.Generic;
using System.IO;
using LumenWorks.Framework.IO.Csv;

namespace Bulldozer.CSV
{
    /// <summary>
    /// multiple csv files may be necessary to upload so this class will be used internally
    /// in place of the Database/List<TableNode></TableNode> which is defined in the base class
    /// </summary>
    public class CSVInstance
    {
        /// <summary>
        /// Available Rock data types
        /// </summary>
        public enum RockDataType
        {
            Business,
            BusinessAddress,
            BusinessAttribute,
            BusinessAttributeValue,
            BusinessContact,
            BusinessPhone,
            Person,
            PersonAddress,
            PersonAttribute,
            PersonAttributeValue,
            PersonPhone,
            PersonSearchKey,
            FAMILY,
            FamilyAttribute,
            USERLOGIN,
            BANKACCOUNT,
            ACCOUNT,
            BATCH,
            PLEDGE,
            CONTRIBUTION,
            SCHEDULEDTRANSACTION,
            NAMEDLOCATION,
            GROUPTYPE,
            GROUP,
            GROUPPOLYGON,
            GROUPMEMBER,
            RELATIONSHIP,
            ATTENDANCE,
            METRICS,
            ENTITYATTRIBUTE,
            ENTITYATTRIBUTEVALUE,
            CONTENTCHANNEL,
            CONTENTCHANNELITEM,
            NOTE,
            PRAYERREQUEST,
            PREVIOUSLASTNAME,
            CONNECTIONREQUEST,
            BENEVOLENCEREQUEST,
            BENEVOLENCERESULT,
            PERSONHISTORY,
            NONE
        };

        /// <summary>
        /// Gets or sets the type of the record.
        /// </summary>
        /// <value>
        /// The type of the record.
        /// </value>
        public RockDataType RecordType
        {
            get;
            set;
        }

        /// <summary>
        /// Rock Address Types
        /// </summary>
        /// <value>
        /// The type of Address.
        /// </value>
        public enum AddressType
        {
            Home = 0,
            Work = 1,
            Previous = 2,
            Other = 3
        }

        /// <summary>
        /// Rock Family Group Roles
        /// </summary>
        /// <value>
        /// The Group Role of a Family member.
        /// </value>

        public enum FamilyRole
        {
            Adult = 0,
            Child = 1
        }

        /// <summary>
        /// Rock Person Search Key types.
        /// </summary>
        /// <value>
        /// The type of Person Search Key.
        /// </value>
        public enum PersonSearchKeyType
        {
            Email = 0,
            AlternateId = 1
        }

        public enum RecordStatus
        {
            Active = 0,
            Inactive = 1,
            Pending = 2
        }

        public enum ImportUpdateType
        {
            AlwaysUpdate,
            AddOnly,
            MostRecentWins
        }

        /// <summary>
        /// Holds a reference to the loaded nodes
        /// </summary>
        public List<DataNode> TableNodes;

        /// <summary>
        /// The local database
        /// </summary>
        public CsvReader Database;

        /// <summary>
        /// Gets or sets the name of the file.
        /// </summary>
        /// <value>
        /// The name of the file.
        /// </value>
        public string FileName { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CSVInstance"/> class.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        public CSVInstance( string fileName )
        {
            RecordType = RockDataType.FAMILY; //default to family, import changes based on filename.

            FileName = fileName;

            // reset the reader so we don't skip the first row
            Database = new CsvReader( new StreamReader( fileName ), true );
        }
    }
}