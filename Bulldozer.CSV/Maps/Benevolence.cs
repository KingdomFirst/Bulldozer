// <copyright>
// Copyright 2021 by Kingdom First Solutions
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
using System.Data.Entity;
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
    /// Partial of CSVComponent that holds the Benevolence related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Benevolence Methods

        /// <summary>
        /// Loads the Benevolence Requests data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadBenevolenceRequest( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedBenevolenceRequests = new BenevolenceRequestService( lookupContext ).Queryable().Count( p => p.ForeignKey != null );
            var requestStatusDTGuid = Rock.SystemGuid.DefinedType.BENEVOLENCE_REQUEST_STATUS.AsGuid();
            var requestStatusPendingDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.BENEVOLENCE_PENDING ), lookupContext ).Id;
            var homePhoneTypeDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.PERSON_PHONE_TYPE_HOME ), lookupContext ).Id;
            var mobilePhoneTypeDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.PERSON_PHONE_TYPE_MOBILE ), lookupContext ).Id;
            var workPhoneTypeDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.PERSON_PHONE_TYPE_WORK ), lookupContext ).Id;

            var benevolenceRequestList = new List<BenevolenceRequest>();

            var completedItems = 0;
            var addedItems = 0;
            ReportProgress( 0, string.Format( "Verifying benevolence request import ({0:N0} already imported).", importedBenevolenceRequests ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            var ben = new BenevolenceRequest();
            var prayer = new PrayerRequest();

            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var benevolenceRequestText = row[BenevolenceRequestText] as string;
                var benevolenceRequestDate = row[BenevolenceRequestDate] as string;
                var benevolenceRequestId = row[BenevolenceRequestId] as string;
                var benevolenceRequestFirstName = row[BenevolenceRequestFirstName] as string;
                var benevolenceRequestLastName = row[BenevolenceRequestLastName] as string;
                var benevolenceRequestEmail = row[BenevolenceRequestEmail] as string;
                var benevolenceRequestCreatedById = row[BenevolenceRequestCreatedById] as string;
                var benevolenceRequestCreatedDate = row[BenevolenceRequestCreatedDate] as string;
                var benevolenceRequestRequestedById = row[BenevolenceRequestRequestedById] as string;
                var benevolenceRequestCaseWorkerId = row[BenevolenceRequestCaseWorkerId] as string;
                var benevolenceRequestCellPhone = row[BenevolenceRequestCellPhone] as string;
                var benevolenceRequestHomePhone = row[BenevolenceRequestHomePhone] as string;
                var benevolenceRequestWorkPhone = row[BenevolenceRequestWorkPhone] as string;
                var benevolenceRequestGovernmentId = row[BenevolenceRequestGovernmentId] as string;
                var benevolenceRequestProvidedNextSteps = row[BenevolenceRequestProvidedNextSteps] as string;
                var benevolenceRequestStatus = row[BenevolenceRequestStatus] as string;
                var benevolenceRequestResultSummary = row[BenevolenceRequestResultSummary] as string;
                var benevolenceRequestAddress = row[BenevolenceRequestAddress] as string;
                var benevolenceRequestAddress2 = row[BenevolenceRequestAddress2] as string;
                var benevolenceRequestCity = row[BenevolenceRequestCity] as string;
                var benevolenceRequestState = row[BenevolenceRequestState] as string;
                var benevolenceRequestZip = row[BenevolenceRequestZip] as string;
                var benevolenceRequestCountry = row[BenevolenceRequestCountry] as string;

                if ( !string.IsNullOrWhiteSpace( benevolenceRequestText )
                    && ( !string.IsNullOrWhiteSpace( benevolenceRequestRequestedById )
                        || ( !string.IsNullOrWhiteSpace( benevolenceRequestFirstName ) && !string.IsNullOrWhiteSpace( benevolenceRequestLastName ) )
                        )
                    )
                {
                    var email = string.Empty;
                    var firstName = benevolenceRequestFirstName;
                    var lastName = benevolenceRequestLastName;
                    if ( benevolenceRequestEmail.IsEmail() )
                    {
                        email = benevolenceRequestEmail;
                    }

                    int? requestedByAliasId = null;
                    var requestedByPersonKeys = GetPersonKeys( benevolenceRequestRequestedById );
                    if ( requestedByPersonKeys != null )
                    {
                        requestedByAliasId = requestedByPersonKeys.PersonAliasId;
                    }

                    int? createdByAliasId = null;
                    var createdByPersonKeys = GetPersonKeys( benevolenceRequestCreatedById );
                    if ( createdByPersonKeys != null )
                    {
                        createdByAliasId = createdByPersonKeys.PersonAliasId;
                    }

                    int? caseWorkerAliasId = null;
                    var caseWorkerPersonKeys = GetPersonKeys( benevolenceRequestCaseWorkerId );
                    if ( caseWorkerPersonKeys != null )
                    {
                        caseWorkerAliasId = caseWorkerPersonKeys.PersonAliasId;
                    }

                    var benevolenceRequest = AddBenevolenceRequest( lookupContext, benevolenceRequestText, benevolenceRequestDate, benevolenceRequestFirstName, benevolenceRequestLastName,
                        email, benevolenceRequestCellPhone, benevolenceRequestHomePhone, benevolenceRequestWorkPhone, benevolenceRequestResultSummary,
                        benevolenceRequestCreatedDate, benevolenceRequestId, createdByAliasId, requestedByAliasId, caseWorkerAliasId,
                        benevolenceRequestGovernmentId, benevolenceRequestProvidedNextSteps, false );

                    if ( benevolenceRequest.Id == 0 )
                    {
                        // Handle request Status
                        if ( !string.IsNullOrWhiteSpace( benevolenceRequestStatus ) )
                        {
                            var statusDV = FindDefinedValueByTypeAndName( lookupContext, requestStatusDTGuid, benevolenceRequestStatus );
                            if ( statusDV == null )
                            {
                                statusDV = AddDefinedValue( new RockContext(), requestStatusDTGuid.ToString(), benevolenceRequestStatus );
                            }
                        }
                        else
                        {
                            // set default status to pending
                            benevolenceRequest.RequestStatusValueId = requestStatusPendingDVId;
                        }

                        // Check for requester person and use its info instead
                        if ( requestedByAliasId.HasValue && requestedByAliasId.Value > 0 )
                        {
                            Person requester = null;
                            var requesterPersonAlias = new PersonAliasService( lookupContext ).Queryable()
                                                            .AsNoTracking()
                                                            .FirstOrDefault( pa => pa.Id == requestedByAliasId.Value );
                            if ( requesterPersonAlias != null && requesterPersonAlias.PersonId > 0 )
                            {
                                requester = requesterPersonAlias.Person;
                            }

                            if ( requester != null )
                            {
                                if ( !string.IsNullOrWhiteSpace( requester.NickName ) )
                                {
                                    benevolenceRequest.FirstName = requester.NickName;
                                }
                                else if ( !string.IsNullOrWhiteSpace( requester.FirstName ) )
                                {
                                    benevolenceRequest.FirstName = requester.FirstName;
                                }
                                if ( !string.IsNullOrWhiteSpace( requester.LastName ) )
                                {
                                    benevolenceRequest.LastName = requester.LastName;
                                }
                                if ( !string.IsNullOrWhiteSpace( requester.Email ) )
                                {
                                    benevolenceRequest.Email = requester.Email;
                                }
                                if ( requester.PrimaryCampusId.HasValue )
                                {
                                    benevolenceRequest.CampusId = requester.PrimaryCampusId;
                                }
                                if ( requester.PhoneNumbers.Any( n => n.NumberTypeValueId.Value == homePhoneTypeDVId ) )
                                {
                                    benevolenceRequest.HomePhoneNumber = requester.PhoneNumbers.FirstOrDefault( n => n.NumberTypeValueId.Value == homePhoneTypeDVId ).NumberFormatted;
                                }
                                if ( requester.PhoneNumbers.Any( n => n.NumberTypeValueId.Value == mobilePhoneTypeDVId ) )
                                {
                                    benevolenceRequest.CellPhoneNumber = requester.PhoneNumbers.FirstOrDefault( n => n.NumberTypeValueId.Value == mobilePhoneTypeDVId ).NumberFormatted;
                                }
                                if ( requester.PhoneNumbers.Any( n => n.NumberTypeValueId.Value == workPhoneTypeDVId ) )
                                {
                                    benevolenceRequest.WorkPhoneNumber = requester.PhoneNumbers.FirstOrDefault( n => n.NumberTypeValueId.Value == workPhoneTypeDVId ).NumberFormatted;
                                }
                                var requesterAddressLocation = requester.GetHomeLocation();
                                if ( requesterAddressLocation != null )
                                {
                                    benevolenceRequest.LocationId = requesterAddressLocation.Id;
                                }
                            }
                            else
                            {
                                benevolenceRequestRequestedById = null;
                            }
                        }
                        if ( string.IsNullOrWhiteSpace( benevolenceRequestRequestedById ) )
                        {
                            // Handle Address
                            var requestAddress = new LocationService( lookupContext ).Get( benevolenceRequestAddress.Left( 100 ), benevolenceRequestAddress2.Left( 100 ), benevolenceRequestCity, benevolenceRequestState, benevolenceRequestZip, benevolenceRequestCountry, verifyLocation: false );
                            if ( requestAddress != null )
                            {
                                benevolenceRequest.LocationId = requestAddress.Id;
                            }
                        }
                        benevolenceRequestList.Add( benevolenceRequest );
                        addedItems++;
                    }
                    completedItems++;
                    if ( completedItems % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} benevolence requests processed.", completedItems ) );
                    }

                    if ( completedItems % ReportingNumber < 1 )
                    {
                        SaveBenevolenceRequests( benevolenceRequestList );
                        ReportPartialProgress();
                        benevolenceRequestList.Clear();
                    }
                }
            }

            if ( benevolenceRequestList.Any() )
            {
                SaveBenevolenceRequests( benevolenceRequestList );
            }

            ReportProgress( 100, string.Format( "Finished benevolence request import: {0:N0} benevolence requests processed, {1:N0} imported.", completedItems, addedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the benevolence requests.
        /// </summary>
        /// <param name="benevolenceRequestList">The prayer request list.</param>
        private static void SaveBenevolenceRequests( List<BenevolenceRequest> benevolenceRequestList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.BenevolenceRequests.AddRange( benevolenceRequestList );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

        #endregion
    }
}
