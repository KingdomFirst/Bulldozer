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
using System;
using System.Collections.Generic;
using System.Linq;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Note related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region ConnectionRequest Methods

        /// <summary>
        /// Loads the ConnectionRequests.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadConnectionRequest( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var connectionTypes = new ConnectionTypeService( lookupContext ).Queryable().ToList();
            var activityTypes = new ConnectionActivityTypeService( lookupContext ).Queryable().ToList();
            var opportunities = new ConnectionOpportunityService( lookupContext ).Queryable().ToList();
            var statuses = new ConnectionStatusService( lookupContext ).Queryable().ToList();
            var requests = new ConnectionRequestService( lookupContext ).Queryable().ToList();

            var newRequests = new List<ConnectionRequest>();
            var newActivities = new List<ConnectionRequestActivity>();

            var completedItems = 0;
            ReportProgress( 0, string.Format( "Verifying connection request import ({0:N0} already imported).", requests.Count( n => n.ForeignKey != null ) ) );

            ConnectionType connectionType = null;
            ConnectionOpportunity opportunity = null;
            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var oForeignKey = row[OpportunityForeignKey] as string;
                var oName = row[OpportunityName] as string;
                var cType = row[ConnectionType] as string;
                var oDescription = row[OpportunityDescription] as string;
                var oActive = row[OpportunityActive].AsBoolean();
                var oCreatedDate = row[OpportunityCreated].AsDateTime();
                var oModifiedDate = row[OpportunityModified].AsDateTime();

                var rForeignKey = row[RequestForeignKey] as string;
                var rPersonId = row[RequestPersonId].AsIntegerOrNull();
                var rConnectorId = row[RequestConnectorId].AsIntegerOrNull();
                var rCreatedDate = row[RequestCreated].AsDateTime();
                var rModifiedDate = row[RequestModified].AsDateTime();
                var rStatus = row[RequestStatus] as string;
                var rState = row[RequestState].AsIntegerOrNull();
                var rComments = row[RequestComments] as string;
                var rFollowUp = row[RequestFollowUp].AsDateTime();

                var aType = row[ActivityType] as string;
                var aNote = row[ActivityNote] as string;
                var aCreatedDate = row[ActivityDate].AsDateTime();
                var aConnectorId = row[ActivityConnectorId].AsIntegerOrNull();

                // lookup or reuse connection type
                if ( connectionType == null || !connectionType.Name.Equals( cType, StringComparison.OrdinalIgnoreCase ) )
                {
                    connectionType = connectionTypes.FirstOrDefault( t => t.Name.Equals( cType, StringComparison.OrdinalIgnoreCase ) );
                }

                if ( connectionType == null )
                {
                    connectionType = AddConnectionType( lookupContext, cType );
                    connectionTypes.Add( connectionType );
                }

                if ( connectionType != null && !string.IsNullOrWhiteSpace( oName ) && GetPersonKeys( rPersonId ) != null )
                {
                    // lookup, reuse, or create connection opportunity
                    if ( opportunity == null || !opportunity.ForeignKey.Equals( oForeignKey, StringComparison.OrdinalIgnoreCase ) )
                    {
                        opportunity = opportunities.FirstOrDefault( o => ( o.ForeignKey != null && o.ForeignKey.Equals( oForeignKey, StringComparison.OrdinalIgnoreCase ) ) || o.Name.Equals( oName, StringComparison.OrdinalIgnoreCase ) );
                    }

                    if ( opportunity == null )
                    {
                        opportunity = AddConnectionOpportunity( lookupContext, connectionType.Id, oCreatedDate, oName, oDescription, oActive, oForeignKey );
                        opportunities.Add( opportunity );
                    }
                    else if ( opportunity.ForeignKey == null )
                    {
                        opportunity.ForeignKey = oForeignKey;
                        opportunity.ForeignId = oForeignKey.AsIntegerOrNull();
                        lookupContext.SaveChanges();
                    }

                    // lookup, reuse, or create connection request
                    var requestStatus = statuses.FirstOrDefault( s => s.Name.Equals( rStatus, StringComparison.OrdinalIgnoreCase ) && s.ConnectionTypeId.HasValue && s.ConnectionTypeId.Value == connectionType.Id );
                    if ( requestStatus == null )
                    {
                        requestStatus = AddConnectionStatus( lookupContext, rStatus, connectionType.Id );
                        statuses.Add( requestStatus );
                    }

                    var requestor = GetPersonKeys( rPersonId );
                    var requestConnector = rConnectorId.HasValue ? GetPersonKeys( rConnectorId ) : null;
                    var request = requests.FirstOrDefault( r => r.ForeignKey != null && r.ForeignKey.Equals( rForeignKey, StringComparison.OrdinalIgnoreCase ) )
                        ?? newRequests.FirstOrDefault( r => r.ForeignKey != null && r.ForeignKey.Equals( rForeignKey, StringComparison.OrdinalIgnoreCase ) );

                    if ( request == null && requestor != null && requestStatus != null )
                    {
                        request = new ConnectionRequest
                        {
                            ConnectionOpportunityId = opportunity.Id,
                            PersonAliasId = requestor.PersonAliasId,
                            Comments = rComments,
                            ConnectionStatusId = requestStatus.Id,
                            ConnectionState = ( ConnectionState ) rState,
                            ConnectorPersonAliasId = requestConnector?.PersonAliasId,
                            ConnectionTypeId = connectionType.Id,
                            FollowupDate = rFollowUp,
                            CreatedDateTime = rCreatedDate,
                            ModifiedDateTime = rModifiedDate,
                            ForeignKey = rForeignKey,
                            ForeignId = rForeignKey.AsIntegerOrNull(),
                            ConnectionRequestActivities = new List<ConnectionRequestActivity>()
                        };
                        newRequests.Add( request );
                    }

                    // create activity
                    if ( !string.IsNullOrWhiteSpace( aType ) )
                    {
                        var activityConnector = aConnectorId.HasValue ? GetPersonKeys( aConnectorId ) : null;
                        var activityType = activityTypes.FirstOrDefault( t => t.Name.Equals( aType, StringComparison.OrdinalIgnoreCase ) );
                        if ( request != null && activityType != null )
                        {
                            var activity = AddConnectionActivity( opportunity.Id, aNote, aCreatedDate, activityConnector?.PersonAliasId, activityType.Id, rForeignKey );

                            if ( request.Id > 0 )
                            {
                                activity.ConnectionRequestId = request.Id;
                                newActivities.Add( activity );
                            }
                            else
                            {
                                request.ConnectionRequestActivities.Add( activity );
                            }
                        }
                    }

                    completedItems++;
                    if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} requests processed.", completedItems ) );
                    }

                    if ( completedItems % DefaultChunkSize < 1 )
                    {
                        SaveConnectionRequests( newRequests, newActivities );
                        ReportPartialProgress();
                        requests.AddRange( newRequests );
                        newRequests.Clear();
                        newActivities.Clear();
                    }
                }
            }

            if ( newRequests.Count > 0 || newActivities.Count > 0 )
            {
                SaveConnectionRequests( newRequests, newActivities );
            }

            ReportProgress( 100, string.Format( "Finished connection request import: {0:N0} requests imported.", completedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the connection requests.
        /// </summary>
        /// <param name="noteList">The note list.</param>
        private static void SaveConnectionRequests( List<ConnectionRequest> requestList, List<ConnectionRequestActivity> activityList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                // new requests include activities
                rockContext.ConnectionRequests.AddRange( requestList );
                // existing requests, save new activities
                rockContext.ConnectionRequestActivities.AddRange( activityList );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }
    }

    #endregion
}