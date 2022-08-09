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
            Description = "Ghanaian Phone Number",
            MatchExpression = @"^(\d{2})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData GhanaWithCountry = new CountryCodeData
        {
            CountryCode = 233,
            Description = "Ghanaian Phone Number with Country Code",
            MatchExpression = @"^233(\d{2})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData Mexico = new CountryCodeData
        {
            CountryCode = 52,
            Description = "Mexican Phone Number",
            MatchExpression = @"^(\d{3})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData MexicoWithCountry = new CountryCodeData
        {
            CountryCode = 52,
            Description = "Mexican Phone Number with Country Code",
            MatchExpression = @"^52(\d{3})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData MexicoMobile = new CountryCodeData
        {
            CountryCode = 52,
            Description = "Mexican Mobile Phone Number",
            MatchExpression = @"^(1)(\d{3})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3 $4"
        };

        public static CountryCodeData MexicoMobileWithCountry = new CountryCodeData
        {
            CountryCode = 52,
            Description = "Mexican Mobile Phone Number with Country Code",
            MatchExpression = @"^52(1)(\d{3})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3 $4"
        };

        public static CountryCodeData NewZealand = new CountryCodeData
        {
            CountryCode = 64,
            Description = "New Zealandic Phone Number",
            MatchExpression = @"^(\d{1})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData NewZealandWithCountry = new CountryCodeData
        {
            CountryCode = 64,
            Description = "New Zealandic Phone Number with Country Code",
            MatchExpression = @"^64(\d{1})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData NewZealand9DigitMobile = new CountryCodeData
        {
            CountryCode = 64,
            Description = "New Zealandic 9 Digit Mobile Phone Number",
            MatchExpression = @"^(2\d{1})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData NewZealand9DigitMobileWithCountry = new CountryCodeData
        {
            CountryCode = 64,
            Description = "New Zealandic 9 Digit Mobile Phone Number with Country Code",
            MatchExpression = @"^64(2\d{1})(\d{3})(\d{4})$",
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

        public static CountryCodeData UnitedArabEmirates = new CountryCodeData
        {
            CountryCode = 971,
            Description = "Emirati Phone Number",
            MatchExpression = @"^(\d{1})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData UnitedArabEmiratesWithCountry = new CountryCodeData
        {
            CountryCode = 971,
            Description = "Emirati Phone Number with Country Code",
            MatchExpression = @"^971(\d{1})(\d{3})(\d{4})$",
            FormatExpression = @"$1 $2 $3"
        };

        public static CountryCodeData UnitedArabEmiratesMobile = new CountryCodeData
        {
            CountryCode = 971,
            Description = "Emirati Mobile Phone Number",
            MatchExpression = @"^(5\d{1})(\d{7})$",
            FormatExpression = @"$1 $2"
        };

        public static CountryCodeData UnitedArabEmiratesMobileWithCountry = new CountryCodeData
        {
            CountryCode = 971,
            Description = "Emirati Mobile Phone Number with Country Code",
            MatchExpression = @"^971(5\d{1})(\d{7})$",
            FormatExpression = @"$1 $2"
        };

        public static CountryCodeData UnitedKingdom = new CountryCodeData
        {
            CountryCode = 44,
            Description = "British Phone Number",
            MatchExpression = @"^(\d{4})(\d{6})$",
            FormatExpression = @"$1 $2"
        };

        public static CountryCodeData UnitedKingdomWithCountry = new CountryCodeData
        {
            CountryCode = 44,
            Description = "British Phone Number with Country Code",
            MatchExpression = @"^44(\d{4})(\d{6})$",
            FormatExpression = @"$1 $2"
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