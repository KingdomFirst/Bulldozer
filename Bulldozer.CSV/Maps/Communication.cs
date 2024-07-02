// <copyright>
// Copyright 2024 by Kingdom First Solutions
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
using Bulldozer.Model;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Linq;
using static Bulldozer.Utility.CachedTypes;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Communication related import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Loads the Communication data.
        /// </summary>
        private int LoadCommunication()
        {
            this.ReportProgress( 0, "Preparing Communication data for import" );

            var rockContext = new RockContext();
            var importedCommunications = rockContext.Communications.AsNoTracking()
                .Where( c => c.ForeignKey != null && c.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .ToList()
                .ToDictionary( k => k.ForeignKey, v => v );

            var communicationCsvsToProcess =  this.CommunicationCsvList.Where( c => !importedCommunications.ContainsKey( $"{this.ImportInstanceFKPrefix}^{c.CommunicationId}" ) );

            if ( communicationCsvsToProcess.Count() < this.CommunicationCsvList.Count() )
            {
                this.ReportProgress( 0, $"{this.CommunicationCsvList.Count() - communicationCsvsToProcess.Count()} Communication(s) from import already exist and will be skipped." );
            }

            // Slice data into chunks and process
            var workingCommunicationImportList = communicationCsvsToProcess.ToList();
            var communicationsRemainingToProcess = workingCommunicationImportList.Count();
            var completed = 0;

            this.ReportProgress( 0, string.Format( "Begin processing {0} Communication Records...", communicationsRemainingToProcess ) );

            while ( communicationsRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Communications processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingCommunicationImportList.Take( Math.Min( this.DefaultChunkSize, workingCommunicationImportList.Count ) ).ToList();
                    var imported = BulkCommunicationImport( csvChunk, rockContext );
                    completed += imported;
                    communicationsRemainingToProcess -= csvChunk.Count;
                    workingCommunicationImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            if ( this.CommunicationRecipientCsvList.Count > 0 )
            {
                completed += LoadCommunicationRecipient();
            }

            return completed;
        }

        private int BulkCommunicationImport( List<CommunicationCsv> communicationCsvList, RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }

            var newCommunications = new List<Communication>();

            foreach ( var communicationCsv in communicationCsvList )
            {
                var sender = ImportedPeopleKeys.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, communicationCsv.SenderPersonId ) );
                var createdByPerson = ImportedPeopleKeys.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, communicationCsv.CreatedByPersonId ) );

                var newCommunication = new Communication
                {
                    FromName = communicationCsv.FromName,
                    FromEmail = communicationCsv.FromEmail,
                    ReplyToEmail = communicationCsv.ReplyToEmail,
                    BCCEmails = communicationCsv.BCCEmails,
                    Subject = communicationCsv.Subject,
                    Message = communicationCsv.EmailMessage,
                    SMSMessage = communicationCsv.SMSMessage,
                    CommunicationType = ( CommunicationType ) ( int ) communicationCsv.CommunicationTypeEnum,
                    SenderPersonAliasId = sender != null ? sender.PersonAliasId : ImportPersonAliasId,
                    SendDateTime = communicationCsv.SentDateTime.GetValueOrDefault() < RockDateTime.Now ? communicationCsv.SentDateTime.GetValueOrDefault() : new DateTime( 1900, 1, 1 ), 
                    ReviewedDateTime = communicationCsv.SentDateTime.GetValueOrDefault() < RockDateTime.Now ? communicationCsv.SentDateTime.GetValueOrDefault() : new DateTime( 1900, 1, 1 ), 
                    CreatedByPersonAliasId = createdByPerson != null ? createdByPerson.PersonAliasId : ImportPersonAliasId,
                    ForeignId = communicationCsv.CommunicationId.AsIntegerOrNull(),
                    ForeignKey = $"{this.ImportInstanceFKPrefix}^{communicationCsv.CommunicationId}",
                    CreatedDateTime = communicationCsv.CreatedDateTime, 
                    Status = CommunicationStatus.Approved
                };
                newCommunications.Add( newCommunication );
            }

            rockContext.BulkInsert( newCommunications );
            return communicationCsvList.Count;
        }

        /// <summary>
        /// Loads the CommunicationRecipient data.
        /// </summary>
        private int LoadCommunicationRecipient()
        {
            this.ReportProgress( 0, "Preparing CommunicationRecipient data for import" );

            var rockContext = new RockContext();
            var importedCommunications = rockContext.Communications.AsNoTracking()
                .Where( c => c.ForeignKey != null && c.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .ToList()
                .ToDictionary( k => k.ForeignKey, v => v );

            var importedCommunicationRecipients = rockContext.CommunicationRecipients.AsNoTracking()
                .Where( c => c.ForeignKey != null && c.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .ToList()
                .ToDictionary( k => k.ForeignKey, v => v );

            var communicationRecipientCsvsToProcess = this.CommunicationRecipientCsvList.Where( c => !importedCommunicationRecipients.ContainsKey( $"{this.ImportInstanceFKPrefix}^{c.CommunicationRecipientId}" ) );

            if ( communicationRecipientCsvsToProcess.Count() < this.CommunicationRecipientCsvList.Count() )
            {
                this.ReportProgress( 0, $"{this.CommunicationRecipientCsvList.Count() - communicationRecipientCsvsToProcess.Count()} CommunicationRecipients(s) from import already exist and will be skipped." );
            }

            // Slice data into chunks and process
            var workingCommunicationRecipientImportList = communicationRecipientCsvsToProcess.ToList();
            var communicationRecipientsRemainingToProcess = workingCommunicationRecipientImportList.Count();
            var completed = 0;
            var missingPersonIds = new List<string>();
            var missingCommunicationIds = new List<string>();

            this.ReportProgress( 0, string.Format( "Begin processing {0} CommunicationRecipient Records...", communicationRecipientsRemainingToProcess ) );

            while ( communicationRecipientsRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Communication Recipients processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingCommunicationRecipientImportList.Take( Math.Min( this.DefaultChunkSize, workingCommunicationRecipientImportList.Count ) ).ToList();
                    var imported = BulkCommunicationRecipientImport( csvChunk, missingPersonIds, missingCommunicationIds, importedCommunications, rockContext );
                    completed += imported;
                    communicationRecipientsRemainingToProcess -= csvChunk.Count;
                    workingCommunicationRecipientImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            if ( missingPersonIds.Count > 0 )
            {
                LogException( $"CommunicationImport", $"The following invalid RecipientPersonId(s) in the communication recipient csv resulted in the following {missingPersonIds.Count} communication recipient(s) being skipped:\r\n{string.Join( ", ", missingPersonIds )}." );
            }

            if ( missingCommunicationIds.Count > 0 )
            {
                LogException( $"CommunicationImport", $"All recipients for the following {missingCommunicationIds.Count} CommunicationId(s) from the communication recipient csv file were skipped due to invalid CommunicationId:\r\n{string.Join( ", ", missingCommunicationIds )}." );
            }
            return completed;
        }

        private int BulkCommunicationRecipientImport( List<CommunicationRecipientCsv> communicationRecipientCsvList, List<string> missingPersonIds, List<string> missingCommunicationIds, Dictionary<string,Communication> importedCommunications, RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }

            var newCommunicationRecipients = new List<CommunicationRecipient>();

            foreach ( var communicationRecipientCsv in communicationRecipientCsvList )
            {
                var recipient = ImportedPeopleKeys.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, communicationRecipientCsv.RecipientPersonId ) );
                var communication = importedCommunications.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, communicationRecipientCsv.CommunicationId ) );
                
                if ( recipient == null )
                {
                    missingPersonIds.Add( communicationRecipientCsv.RecipientPersonId );
                }
                else if ( communication == null )
                {
                    missingCommunicationIds.Add( communicationRecipientCsv.CommunicationId );
                }
                else
                {
                    var newCommunicationRecipient = new CommunicationRecipient
                    {
                        CommunicationId = communication.Id,
                        PersonAliasId = recipient.PersonAliasId,
                        Status = ( CommunicationRecipientStatus ) ( int ) communicationRecipientCsv.RecipientStatusEnum.GetValueOrDefault(),
                        SendDateTime = communicationRecipientCsv.SentDateTime.GetValueOrDefault() < RockDateTime.Now ? communicationRecipientCsv.SentDateTime.GetValueOrDefault() : new DateTime( 1900, 1, 1 ),
                        ForeignId = communicationRecipientCsv.CommunicationRecipientId.AsIntegerOrNull(),
                        ForeignKey = $"{this.ImportInstanceFKPrefix}^{communicationRecipientCsv.CommunicationRecipientId}"
                    };
                    newCommunicationRecipients.Add( newCommunicationRecipient );
                }
            }

            rockContext.BulkInsert( newCommunicationRecipients );
            return communicationRecipientCsvList.Count;
        }

    }
}
