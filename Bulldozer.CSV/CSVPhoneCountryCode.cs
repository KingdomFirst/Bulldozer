// <copyright>
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
    /// Collection of objects to build CountryCode defined values.
    /// </summary>
    public class CSVPhoneCountryCode
    {
        public static CountryCodeData AustraliaMobile = new CountryCodeData
        {
            CountryCode = 61,
            Description = "Australia Mobile Phone Number",
            MatchExpression = @"^(\d{3})(\d{3})(\d{3})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData AustraliaMobileWithCountry = new CountryCodeData
        {
            CountryCode = 61,
            Description = "Australia Mobile Phone Number with Country Code",
            MatchExpression = @"^61(\d{3})(\d{3})(\d{3})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData Cambodia = new CountryCodeData
        {
            CountryCode = 855,
            Description = "Cambodian Phone Number",
            MatchExpression = @"^(\d{2})(\d{3})(\d{3})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData CambodiaWithCountry = new CountryCodeData
        {
            CountryCode = 855,
            Description = "Cambodian Phone Number with Country Code",
            MatchExpression = @"^855(\d{2})(\d{3})(\d{3})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData GermanLandLineOr3DigitPrefixMobile = new CountryCodeData
        {
            CountryCode = 49,
            Description = "German Land Line or 3 Digit Prefix Mobile Phone Number",
            MatchExpression = @"^(\d{3})(\d{7})$",
            FormatExpression = @"$1 $2"
        };

        public static CountryCodeData GermanLandLineOr3DigitPrefixMobileWithCountry = new CountryCodeData
        {
            CountryCode = 49,
            Description = "German Land Line or 3 Digit Prefix Mobile Phone Number with Country Code",
            MatchExpression = @"^49(\d{3})(\d{7})$",
            FormatExpression = @"$1 $2"
        };

        public static CountryCodeData German4DigitPrefixMobile = new CountryCodeData
        {
            CountryCode = 49,
            Description = "German Mobile Phone Number with 4 Digit Prefix",
            MatchExpression = @"^(\d{4})(\d{7})$",
            FormatExpression = @"$1 $2"
        };

        public static CountryCodeData German4DigitPrefixMobileWithCountry = new CountryCodeData
        {
            CountryCode = 49,
            Description = "German 4 digit prefix Mobile Phone Number with Country Code",
            MatchExpression = @"^49(\d{4})(\d{7})$",
            FormatExpression = @"$1 $2"
        };

        public static CountryCodeData Ghana = new CountryCodeData
        {
            CountryCode = 233,
            Description = "Ghanan Phone Number",
            MatchExpression = @"^(\d{2})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData GhanaWithCountry = new CountryCodeData
        {
            CountryCode = 233,
            Description = "Ghanan Phone Number with Country Code",
            MatchExpression = @"^233(\d{2})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData Niger = new CountryCodeData
        {
            CountryCode = 227,
            Description = "Niger Phone Number",
            MatchExpression = @"^(\d{2})(\d{2})(\d{2})(\d{2})$",
            FormatExpression = @"$1 $2 $3 $4"
        };

        public static CountryCodeData NigerWithCountry = new CountryCodeData
        {
            CountryCode = 227,
            Description = "Niger Phone Number with Country Code",
            MatchExpression = @"^227(\d{2})(\d{2})(\d{2})(\d{2})$",
            FormatExpression = @"$1 $2 $3 $4"
        };

        public static CountryCodeData Spain = new CountryCodeData
        {
            CountryCode = 34,
            Description = "Spanish Phone Number",
            MatchExpression = @"^(\d{2})(\d{4})(\d{3})$",
            FormatExpression = @"$1 $2-$3"
        };

        public static CountryCodeData SpainWithCountry = new CountryCodeData
        {
            CountryCode = 34,
            Description = "Spanish Phone Number with Country Code",
            MatchExpression = @"^34(\d{2})(\d{4})(\d{3})$",
            FormatExpression = @"$1 $2-$3"
        };
    }

    public class CountryCodeData
    {
        public int CountryCode { get; set; }

        public string Description { get; set; }

        public string MatchExpression { get; set; }

        public string FormatExpression { get; set; }
    }
}