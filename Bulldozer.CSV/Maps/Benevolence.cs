// <copyright>
// Copyright 2023 by Kingdom First Solutions
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
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Benevolence related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region BenevolenceRequest Methods

        /// <summary>
        /// Loads the Benevolence Requests data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadBenevolenceRequest( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var benevolenceRequestService = new BenevolenceRequestService( lookupContext );
            var importedBenevolenceRequests = benevolenceRequestService.Queryable().Count( p => p.ForeignKey != null && p.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) );
            var requestStatusDTGuid = Rock.SystemGuid.DefinedType.BENEVOLENCE_REQUEST_STATUS.AsGuid();
            var requestStatusPendingDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.BENEVOLENCE_PENDING ), lookupContext ).Id;
            var homePhoneTypeDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.PERSON_PHONE_TYPE_HOME ), lookupContext ).Id;
            var mobilePhoneTypeDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.PERSON_PHONE_TYPE_MOBILE ), lookupContext ).Id;
            var workPhoneTypeDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.PERSON_PHONE_TYPE_WORK ), lookupContext ).Id;
            var benevolenceTypeService = new BenevolenceTypeService( lookupContext );
            var defaultBenevolenceTypeId = benevolenceTypeService.Get( new Guid( Rock.SystemGuid.BenevolenceType.BENEVOLENCE ) ).Id;

            var benevolenceRequestList = new List<BenevolenceRequest>();

            var completedItems = 0;
            var addedItems = 0;
            ReportProgress( 0, string.Format( "Verifying benevolence request import ({0:N0} already imported).", importedBenevolenceRequests ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately

            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var benevolenceRequestText = row[BenevolenceRequestText];
                var benevolenceRequestDate = row[BenevolenceRequestDate];
                var benevolenceRequestId = row[BenevolenceRequestId];
                var benevolenceRequestFirstName = row[BenevolenceRequestFirstName];
                var benevolenceRequestLastName = row[BenevolenceRequestLastName];
                var benevolenceRequestEmail = row[BenevolenceRequestEmail];
                var benevolenceRequestCreatedById = row[BenevolenceRequestCreatedById];
                var benevolenceRequestCreatedDate = row[BenevolenceRequestCreatedDate];
                var benevolenceRequestRequestedById = row[BenevolenceRequestRequestedById];
                var benevolenceRequestCaseWorkerId = row[BenevolenceRequestCaseWorkerId];
                var benevolenceRequestCellPhone = row[BenevolenceRequestCellPhone];
                var benevolenceRequestHomePhone = row[BenevolenceRequestHomePhone];
                var benevolenceRequestWorkPhone = row[BenevolenceRequestWorkPhone];
                var benevolenceRequestGovernmentId = row[BenevolenceRequestGovernmentId];
                var benevolenceRequestProvidedNextSteps = row[BenevolenceRequestProvidedNextSteps];
                var benevolenceRequestStatus = row[BenevolenceRequestStatus];
                var benevolenceRequestResultSummary = row[BenevolenceRequestResultSummary];
                var benevolenceRequestAddress = row[BenevolenceRequestAddress];
                var benevolenceRequestAddress2 = row[BenevolenceRequestAddress2];
                var benevolenceRequestCity = row[BenevolenceRequestCity];
                var benevolenceRequestState = row[BenevolenceRequestState];
                var benevolenceRequestZip = row[BenevolenceRequestZip];
                var benevolenceRequestCountry = row[BenevolenceRequestCountry];
                var benevolenceType = row[BenevolenceType];

                //
                // Verify we have the minimum required information for a valid BenevolenceRequest in the csv file.
                //
                if ( string.IsNullOrWhiteSpace( benevolenceRequestText )
                    || ( string.IsNullOrWhiteSpace( benevolenceRequestRequestedById ) && ( string.IsNullOrWhiteSpace( benevolenceRequestFirstName ) || string.IsNullOrWhiteSpace( benevolenceRequestLastName ) ) ) )
                {
                    ReportProgress( 0, $"Benevolence Request {benevolenceRequestId} is missing information. See exception log for details." );
                    LogException( "InvalidBenevolenceRequest", string.Format( "RequestId: {0} - BenevolenceRequestText and either BenevolenceRequestRequestedById or both BenevolenceRequestFirstName and BenevolenceRequestLastName are required. Benevolence Request {0} was not imported.", benevolenceRequestId ) );
                    completedItems++;
                    continue;
                }

                //
                // Check that this Benevolence Request doesn't already exist.
                //
                var exists = false;
                if ( importedBenevolenceRequests > 0 )
                {
                    exists = benevolenceRequestService.Queryable().AsNoTracking().Any( r => r.ForeignKey == this.ImportInstanceFKPrefix + "^" + benevolenceRequestId );
                }

                if ( !exists )
                {
                    var email = string.Empty;
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

                    var requestDate = ( DateTime ) ParseDateOrDefault( benevolenceRequestDate, Bulldozer.BulldozerComponent.ImportDateTime );
                    var dateCreated = ( DateTime ) ParseDateOrDefault( benevolenceRequestCreatedDate, Bulldozer.BulldozerComponent.ImportDateTime );

                    var benevolenceTypeId = defaultBenevolenceTypeId;
                    if ( !benevolenceType.IsNullOrWhiteSpace() )
                    {
                        var existingBenevolenceType = benevolenceTypeService.Queryable().FirstOrDefault( t => t.Name == benevolenceType );
                        if ( existingBenevolenceType != null )
                        {
                            benevolenceTypeId = existingBenevolenceType.Id;
                        }
                        else
                        {
                            benevolenceTypeId = AddBenevolenceType( lookupContext, benevolenceType );
                        }
                    }

                    var benevolenceRequest = new BenevolenceRequest
                    {
                        RequestedByPersonAliasId = requestedByAliasId,
                        FirstName = benevolenceRequestFirstName,
                        LastName = benevolenceRequestLastName,
                        Email = email,
                        RequestText = benevolenceRequestText,
                        RequestDateTime = requestDate,
                        CreatedDateTime = dateCreated,
                        CreatedByPersonAliasId = createdByAliasId,
                        ResultSummary = benevolenceRequestResultSummary,
                        CaseWorkerPersonAliasId = caseWorkerAliasId,
                        ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, benevolenceRequestId ),
                        ForeignId = benevolenceRequestId.AsType<int?>(),
                        BenevolenceTypeId = benevolenceTypeId
                    };

                    // Handle request Status
                    if ( !string.IsNullOrWhiteSpace( benevolenceRequestStatus ) )
                    {
                        var statusDV = FindDefinedValueByTypeAndName( lookupContext, requestStatusDTGuid, benevolenceRequestStatus );
                        if ( statusDV == null )
                        {
                            statusDV = AddDefinedValue( new RockContext(), requestStatusDTGuid.ToString(), benevolenceRequestStatus );
                        }
                        benevolenceRequest.RequestStatusValueId = statusDV.Id;
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
                            if ( requester.ConnectionStatusValueId.HasValue )
                            {
                                benevolenceRequest.ConnectionStatusValueId = requester.ConnectionStatusValueId;
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
                        try
                        {
                            Location requestAddress = GetOrAddLocation( lookupContext, benevolenceRequestAddress, benevolenceRequestAddress2, benevolenceRequestCity, benevolenceRequestState, benevolenceRequestZip, benevolenceRequestCountry );

                            if ( requestAddress != null )
                            {
                                benevolenceRequest.LocationId = requestAddress.Id;
                            }
                        }
                        catch ( Exception ex )
                        {
                            LogException( "Benevolence Import", string.Format( "Error Importing Address for Request \"{0}\". {1}", benevolenceRequestId, ex.Message ) );
                        }
                    }
                    benevolenceRequestList.Add( benevolenceRequest );
                    addedItems++;
                }
                completedItems++;
                if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} benevolence requests processed.", completedItems ) );
                }

                if ( completedItems % DefaultChunkSize < 1 )
                {
                    SaveBenevolenceRequests( benevolenceRequestList );
                    ReportPartialProgress();
                    benevolenceRequestList.Clear();
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
        /// <param name="benevolenceRequestList">The benevolence request list.</param>
        private static void SaveBenevolenceRequests( List<BenevolenceRequest> benevolenceRequestList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.BenevolenceRequests.AddRange( benevolenceRequestList );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

        #endregion BenevolenceRequest Methods

        #region BenevolenceResult Methods

        /// <summary>
        /// Loads the BenevolenceResult data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadBenevolenceResult( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var benevolenceResultService = new BenevolenceResultService( lookupContext );
            var benevolenceRequestService = new BenevolenceRequestService( lookupContext );
            var resultTypeDTGuid = Rock.SystemGuid.DefinedType.BENEVOLENCE_RESULT_TYPE.AsGuid();
            var benevolenceResultList = new List<BenevolenceResult>();

            var importedRequestIds = new List<int>();

            var completed = 0;
            var importedCount = 0;
            var alreadyImportedCount = benevolenceResultService.Queryable().AsNoTracking().Count( i => i.ForeignKey != null && i.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) );
            ReportProgress( 0, $"Starting Benevolence Result import ({alreadyImportedCount:N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var benevolenceResultRequestId = row[BenevolenceResultRequestId];
                var benevolenceResultType = row[BenevolenceResultType];
                var benevolenceResultId = row[BenevolenceResultId];
                var benevolenceResultAmount = row[BenevolenceResultAmount];
                var benevolenceResultSummary = row[BenevolenceResultSummary];
                var benevolenceResultCreatedById = row[BenevolenceResultCreatedById];
                var benevolenceResultCreatedDate = row[BenevolenceResultCreatedDate];

                //
                // Verify the Benevolence Result has a Result Type provided in the csv file.
                //
                if ( string.IsNullOrWhiteSpace( benevolenceResultType ) )
                {
                    ReportProgress( 0, $"Benevolence Result {benevolenceResultId} has no BenevolenceResultType value provided. Skipping Benevolence Result {benevolenceResultId}." );
                    LogException( "InvalidBenevolenceResult", string.Format( "ResultId: {0} - Missing BenevolenceResultType value. Benevolence Result {0} was not imported.", benevolenceResultId ) );
                    completed++;
                    continue;
                }

                BenevolenceRequest benevolenceRequest = null;
                if ( benevolenceRequestService.Queryable().AsNoTracking().Any( r => r.ForeignKey == this.ImportInstanceFKPrefix + "^" + benevolenceResultRequestId ) )
                {
                    benevolenceRequest = benevolenceRequestService.Queryable().AsNoTracking().FirstOrDefault( r => r.ForeignKey == this.ImportInstanceFKPrefix + "^" + benevolenceResultRequestId );
                }

                //
                // Verify the Benevolence Request exists.
                //
                if ( benevolenceRequest == null || benevolenceRequest.Id < 1 )
                {
                    ReportProgress( 0, $"Benevolence Request {benevolenceResultRequestId} not found. Skipping Benevolence Result {benevolenceResultId}." );
                    LogException( "InvalidBenevolenceResult", string.Format( "ResultId: {0} - BenevolenceResultRequestId {1} does not exist in imported Benevolence Requests. Benevolence Result {0} was not imported.", benevolenceResultId, benevolenceResultRequestId ) );
                    completed++;
                    continue;
                }

                //
                // Check that this Benevolence Result doesn't already exist.
                //
                var exists = false;
                if ( alreadyImportedCount > 0 )
                {
                    exists = benevolenceResultService.Queryable().AsNoTracking().Any( r => r.ForeignKey == this.ImportInstanceFKPrefix + "^" + benevolenceResultId );
                }

                if ( !exists )
                {
                    // Handle Result Type
                    var resultTypeDV = FindDefinedValueByTypeAndName( lookupContext, resultTypeDTGuid, benevolenceResultType );
                    if ( resultTypeDV == null )
                    {
                        resultTypeDV = AddDefinedValue( new RockContext(), resultTypeDTGuid.ToString(), benevolenceResultType );
                    }

                    // Format created date
                    var resultCreatedDate = ( DateTime ) ParseDateOrDefault( benevolenceResultCreatedDate, Bulldozer.BulldozerComponent.ImportDateTime );

                    // Handle created by
                    int? createdByAliasId = null;
                    var createdByPersonKeys = GetPersonKeys( benevolenceResultCreatedById );
                    if ( createdByPersonKeys != null )
                    {
                        createdByAliasId = createdByPersonKeys.PersonAliasId;
                    }

                    //
                    // Create and populate the new Benevolence Result.
                    //
                    var benevolenceResult = new BenevolenceResult
                    {
                        BenevolenceRequestId = benevolenceRequest.Id,
                        ResultSummary = benevolenceResultSummary,
                        ResultTypeValueId = resultTypeDV.Id,
                        Amount = benevolenceResultAmount.AsType<decimal?>(),
                        ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, benevolenceResultId ),
                        ForeignId = benevolenceResultId.AsType<int?>(),
                        CreatedDateTime = resultCreatedDate,
                        CreatedByPersonAliasId = createdByAliasId,
                    };

                    benevolenceResultList.Add( benevolenceResult );

                    importedCount++;
                }

                //
                // Notify user of our status.
                //
                completed++;
                if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed:N0} Benevolence Request records processed, {importedCount:N0} imported." );
                }

                if ( completed % DefaultChunkSize < 1 )
                {
                    SaveBenevolenceResults( benevolenceResultList );
                    ReportPartialProgress();
                    benevolenceResultList.Clear();

                    // Clear out variables
                    benevolenceRequestService = new BenevolenceRequestService( lookupContext );
                }
            }

            if ( benevolenceResultList.Any() )
            {
                SaveBenevolenceResults( benevolenceResultList );
            }

            ReportProgress( 0, $"Finished Benevolence Result import: {importedCount:N0} records added." );

            return completed;
        }

        /// <summary>
        /// Saves the benevolence results.
        /// </summary>
        /// <param name="benevolenceResultList">The benevolence result list.</param>
        private static void SaveBenevolenceResults( List<BenevolenceResult> benevolenceResultList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.BenevolenceResults.AddRange( benevolenceResultList );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

        #endregion BenevolenceResult Methods
    }
}
