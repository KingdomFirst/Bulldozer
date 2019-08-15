// <copyright>
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
using System.Collections.Generic;
using System.Linq;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Phone related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Phone Methods

        /// <summary>
        /// Loads the Phone Number data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadPhoneNumber( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedPhoneNumbers = new PhoneNumberService( lookupContext ).Queryable().Count( n => n.ForeignKey != null );
            var phoneTypeValues = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE ), lookupContext ).DefinedValues;

            var phoneNumberList = new List<PhoneNumber>();
            var skippedPhoneNumbers = new Dictionary<string, string>();

            var completedItems = 0;
            ReportProgress( 0, string.Format( "Verifying phone number import ({0:N0} already imported).", importedPhoneNumbers ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var personKey = row[PhonePersonId] as string;
                var phoneType = row[PhoneType] as string;
                var phoneNumber = row[Phone] as string;
                var phoneKey = row[PhoneId] as string;

                var personKeys = GetPersonKeys( personKey );

                // create user-defined phone type if it doesn't exist
                var phoneTypeId = phoneTypeValues.Where( dv => dv.Value.Equals( phoneType, StringComparison.OrdinalIgnoreCase ) )
                    .Select( dv => ( int? ) dv.Id ).FirstOrDefault();

                if ( !phoneTypeId.HasValue )
                {
                    var newPhoneType = AddDefinedValue( lookupContext, Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE, phoneType );
                    if ( newPhoneType != null )
                    {
                        phoneTypeValues.Add( newPhoneType );
                        phoneTypeId = newPhoneType.Id;
                    }
                }

                if ( personKeys != null && personKeys.PersonId > 0 && !string.IsNullOrWhiteSpace( phoneNumber ) )
                {
                    var isMessagingEnabled = ( bool ) ParseBoolOrDefault( row[PhoneIsMessagingEnabled], false );
                    var isUnlisted = ( bool ) ParseBoolOrDefault( row[PhoneIsUnlisted], false );
                    var phoneId = phoneKey.AsType<int?>();

                    var extension = string.Empty;
                    var countryCode = PhoneNumber.DefaultCountryCode();
                    var normalizedNumber = string.Empty;
                    var countryIndex = phoneNumber.IndexOf( '+' );
                    var extensionIndex = phoneNumber.LastIndexOf( 'x' ) > 0 ? phoneNumber.LastIndexOf( 'x' ) : phoneNumber.Length;
                    if ( countryIndex >= 0 && phoneNumber.Length > ( countryIndex + 3 ) )
                    {
                        countryCode = phoneNumber.Substring( countryIndex, countryIndex + 3 ).AsNumeric();
                        normalizedNumber = phoneNumber.Substring( countryIndex + 3, extensionIndex - 3 ).AsNumeric().TrimStart( new Char[] { '0' } );
                        extension = phoneNumber.Substring( extensionIndex );
                    }
                    else if ( extensionIndex > 0 )
                    {
                        normalizedNumber = phoneNumber.Substring( 0, extensionIndex ).AsNumeric();
                        extension = phoneNumber.Substring( extensionIndex ).AsNumeric();
                    }
                    else
                    {
                        normalizedNumber = phoneNumber.AsNumeric();
                    }

                    if ( !string.IsNullOrWhiteSpace( normalizedNumber ) )
                    {
                        var currentNumber = new PhoneNumber();
                        currentNumber.PersonId = personKeys.PersonId;
                        currentNumber.CountryCode = countryCode;
                        currentNumber.CreatedByPersonAliasId = ImportPersonAliasId;
                        currentNumber.Extension = extension.Left( 20 );
                        currentNumber.Number = normalizedNumber.TrimStart( new char[] { '0' } ).Left( 20 );
                        currentNumber.NumberFormatted = PhoneNumber.FormattedNumber( currentNumber.CountryCode, currentNumber.Number );
                        currentNumber.NumberTypeValueId = phoneTypeId;
                        if ( phoneType.Equals( "Mobile", StringComparison.OrdinalIgnoreCase ) )
                        {
                            currentNumber.IsMessagingEnabled = isMessagingEnabled;
                        }
                        currentNumber.IsUnlisted = isUnlisted;
                        phoneNumberList.Add( currentNumber );

                        completedItems++;
                        if ( completedItems % ( ReportingNumber * 10 ) < 1 )
                        {
                            ReportProgress( 0, string.Format( "{0:N0} phone numbers processed.", completedItems ) );
                        }

                        if ( completedItems % ReportingNumber < 1 )
                        {
                            SavePhoneNumbers( phoneNumberList );
                            ReportPartialProgress();
                            phoneNumberList.Clear();
                        }
                    }
                }
                else
                {
                    skippedPhoneNumbers.Add( phoneKey, phoneType );
                }
            }

            if ( phoneNumberList.Any() )
            {
                SavePhoneNumbers( phoneNumberList );
            }

            if ( skippedPhoneNumbers.Any() )
            {
                ReportProgress( 0, "The following phone numbers could not be imported and were skipped:" );
                foreach ( var key in skippedPhoneNumbers )
                {
                    ReportProgress( 0, string.Format( "{0} phone type for Foreign ID {1}.", key.Value, key ) );
                }
            }

            ReportProgress( 100, string.Format( "Finished phone number import: {0:N0} phone numbers imported.", completedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the Phone Numbers.
        /// </summary>
        /// <param name="phoneNumberList">The list of phone numbers.</param>
        private static void SavePhoneNumbers( List<PhoneNumber> phoneNumberList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.PhoneNumbers.AddRange( phoneNumberList );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }
    }

    #endregion
}