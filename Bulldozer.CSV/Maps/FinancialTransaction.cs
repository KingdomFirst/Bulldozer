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
using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using static Bulldozer.Utility.Extensions;
using static Bulldozer.Utility.CachedTypes;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Financial import methods
    /// </summary>
    public partial class CSVComponent
    {
        /// <summary>
        /// Maps the contribution.
        /// </summary>
        /// <param name="csvData">The table data.</param>
        private int ImportFinancialTransactions()
        {
            ReportProgress( 0, "Preparing FinancialTransaction data for import..." );

            var rockContext = new RockContext();
            var errors = string.Empty;
            Dictionary<string, GroupMember> groupMemberLookup = null;
            Dictionary<string, Group> groupLookup = null;
            if ( this.ImportedBatches == null )
            {
                LoadImportedBatches( rockContext );
            }
            if ( this.ImportedAccounts == null )
            {
                LoadImportedAccounts( rockContext );
            }
            if ( this.FinancialTransactionDetailCsvList.Where( td => !string.IsNullOrWhiteSpace( td.FundraisingGroupId ) || !string.IsNullOrWhiteSpace( td.FundraisingGroupMemberId ) ).Any() )
            {
                LoadGroupMemberDict( rockContext );
                groupMemberLookup = this.GroupMemberDict.ToDictionary( k => k.Key, v => v.Value );
                groupLookup = this.GroupMemberDict.Values.Select( gm => gm.Group ).DistinctBy( g => g.Id ).ToDictionary( k => k.ForeignKey, v => v );
            }

            // Look for financial gateways and create any that don't exist
            var financialGatewayByIdLookup = new FinancialGatewayService( rockContext ).Queryable().ToDictionary( k => k.Id, v => v.Name );
            var csvGateways = this.FinancialTransactionCsvList.Where( t => t.GatewayId.IsNotNullOrWhiteSpace() ).Select( t => t.GatewayId ).Distinct();
            foreach ( var gateway in csvGateways )
            {
                if ( int.TryParse( gateway, out int gatewayId ) && financialGatewayByIdLookup.ContainsKey( gatewayId ) )
                {
                    continue;
                }
                else if ( financialGatewayByIdLookup.ContainsValue( gateway ) )
                {
                    continue;
                }
                else
                {
                    AddFinancialGateway( rockContext, gateway );
                }
            }

            // Refresh gateway lookup
            financialGatewayByIdLookup = new FinancialGatewayService( rockContext ).Queryable().ToDictionary( k => k.Id, v => v.Name );
            var financialGatewayByNameLookup = financialGatewayByIdLookup.ToDictionary( k => k.Value, v => v.Key );

            ReportProgress( 0, string.Format( "Begin processing {0} FinancialTransaction Records...", this.FinancialTransactionCsvList.Count ) );

            int giverAnonymousPersonAliasId = new PersonService( rockContext ).GetOrCreateAnonymousGiverPerson().Aliases.FirstOrDefault().Id;
            var existingImportedTransactions = new FinancialTransactionService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) );
            var scheduledTransactionLookup = new FinancialScheduledTransactionService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) ).ToDictionary( k => k.ForeignKey, v => v.Id );
            var existingImportedTransactionsHash = new HashSet<string>( existingImportedTransactions.Select( a => a.ForeignKey ).ToList() );
            var personAliasIdLookup = ImportedPeopleKeys.ToDictionary( k => k.Key, v => v.Value.PersonAliasId );

            var accountIdLookup = ImportedAccounts.ToDictionary( k => k.Key, v => v.Value.Id );
            var creditCardTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CREDIT_CARD_TYPE ) ).DefinedValues;

            // Slice data into chunks and process
            var transactionsRemainingToProcess = this.FinancialTransactionCsvList.Count;
            var workingTransactionCsvList = this.FinancialTransactionCsvList.ToList();
            var completed = 0;

            while ( transactionsRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} FinancialTransaction records processed." );
                }
                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingTransactionCsvList.Take( Math.Min( this.DefaultChunkSize, workingTransactionCsvList.Count ) ).ToList();
                    var financialTransactionImportList = new List<FinancialTransactionImport>();
                    foreach ( var financialTransactionCsv in csvChunk )
                    {
                        var newFinancialTransactionImport = new FinancialTransactionImport()
                        {
                            FinancialTransactionForeignKey = $"{ImportInstanceFKPrefix}^{financialTransactionCsv.Id}",
                            AuthorizedPersonForeignKey = $"{ImportInstanceFKPrefix}^{financialTransactionCsv.AuthorizedPersonId}",
                            BatchForeignKey = $"{ImportInstanceFKPrefix}^{financialTransactionCsv.BatchId}",
                            Summary = financialTransactionCsv.Summary,
                            TransactionCode = financialTransactionCsv.TransactionCode,
                            TransactionDate = financialTransactionCsv.TransactionDate.ToSQLSafeDate(),
                            CurrencyTypeValueId = this.CurrencyTypeValues[financialTransactionCsv.CurrencyType].Id,
                            TransactionTypeValueId = this.TransactionTypeValues[financialTransactionCsv.TransactionType.ToString()].Id,
                            CreatedByPersonForeignKey = $"{ImportInstanceFKPrefix}^{financialTransactionCsv.CreatedByPersonId}",
                            CreatedDateTime = financialTransactionCsv.CreatedDateTime.ToSQLSafeDate(),
                            ModifiedByPersonForeignKey = $"{ImportInstanceFKPrefix}^{financialTransactionCsv.ModifiedByPersonId}",
                            ModifiedDateTime = financialTransactionCsv.ModifiedDateTime.ToSQLSafeDate(),
                            IsAnonymous = financialTransactionCsv.IsAnonymous.HasValue ? financialTransactionCsv.IsAnonymous.Value : false,
                            FinancialTransactionDetailImports = new List<FinancialTransactionDetailImport>()
                        };

                        if ( financialTransactionCsv.ScheduledTransactionId.IsNotNullOrWhiteSpace() )
                        {
                            newFinancialTransactionImport.ScheduledTransactionId = scheduledTransactionLookup.GetValueOrNull( $"{ImportInstanceFKPrefix}^{financialTransactionCsv.ScheduledTransactionId}" );
                        }

                        if ( financialTransactionCsv.GatewayId.IsNotNullOrWhiteSpace() )
                        {
                            if ( int.TryParse( financialTransactionCsv.GatewayId, out int gatewayId ) && financialGatewayByIdLookup.ContainsKey( gatewayId ) )
                            {
                                newFinancialTransactionImport.FinancialGatewayId = gatewayId;
                            }
                            else
                            {
                                newFinancialTransactionImport.FinancialGatewayId = financialGatewayByNameLookup[financialTransactionCsv.GatewayId];
                            }
                        }

                        if ( financialTransactionCsv.TransactionSource.IsNotNullOrWhiteSpace() )
                        {
                            newFinancialTransactionImport.TransactionSourceValueId = this.TransactionSourceTypeValues[financialTransactionCsv.TransactionSource].Id;
                        }
                        else
                        {
                            // set default source to onsite, exceptions listed below
                            newFinancialTransactionImport.TransactionSourceValueId = this.TransactionSourceTypeValues["On-Site"].Id;
                        }

                        var nonCashAssetDV = this.NonCashAssetTypeValues.GetValueOrNull( financialTransactionCsv.NonCashAssetType );
                        if ( nonCashAssetDV != null )
                        {
                            newFinancialTransactionImport.NonCashAssetValueId = nonCashAssetDV.Id;
                        }

                        var creditCardTypeDV = this.CreditCardTypeValues.GetValueOrNull( financialTransactionCsv.CreditCardType );
                        if ( creditCardTypeDV != null )
                        {
                            newFinancialTransactionImport.CreditCardTypeValueId = creditCardTypeDV.Id;
                        }

                        foreach ( var transactionDetailCsv in financialTransactionCsv.FinancialTransactionDetails )
                        {
                            var newFinancialTransactionDetail = new FinancialTransactionDetailImport()
                            {
                                FinancialAccountForeignKey = $"{ImportInstanceFKPrefix}^{transactionDetailCsv.AccountId}",
                                Amount = transactionDetailCsv.Amount,
                                CreatedByPersonForeignKey = $"{ImportInstanceFKPrefix}^{transactionDetailCsv.CreatedByPersonId}",
                                CreatedDateTime = transactionDetailCsv.CreatedDateTime.ToSQLSafeDate(),
                                FinancialTransactionDetailForeignKey = $"{ImportInstanceFKPrefix}^{transactionDetailCsv.Id}",
                                ModifiedByPersonForeignKey = $"{ImportInstanceFKPrefix}^{transactionDetailCsv.ModifiedByPersonId}",
                                ModifiedDateTime = transactionDetailCsv.ModifiedDateTime.ToSQLSafeDate(),
                                Summary = transactionDetailCsv.Summary
                            };
                            if ( transactionDetailCsv.FundraisingGroupMemberId.IsNotNullOrWhiteSpace() )
                            {
                                var groupMember = groupMemberLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{transactionDetailCsv.FundraisingGroupMemberId}" );
                                if ( groupMember != null )
                                {
                                    newFinancialTransactionDetail.EntityTypeId = GroupMemberEntityTypeId;
                                    newFinancialTransactionDetail.EntityId = groupMember.Id;
                                }
                                else
                                {
                                    errors += $"{DateTime.Now.ToString()},FinancialTransactionDetail,\"Invalid FundraisingGroupMemberId ({transactionDetailCsv.FundraisingGroupMemberId}) for FinancialTransactionDetail {transactionDetailCsv.Id}. The transaction detail will be created, but the connection to the group member will not.\"\r\n";
                                }
                            }
                            else if ( transactionDetailCsv.FundraisingGroupId.IsNotNullOrWhiteSpace() )
                            {
                                var group = groupLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{transactionDetailCsv.FundraisingGroupId}" );
                                if ( group != null )
                                {
                                    newFinancialTransactionDetail.EntityTypeId = GroupEntityTypeId;
                                    newFinancialTransactionDetail.EntityId = group.Id;
                                }
                                else
                                {
                                    errors += $"{DateTime.Now.ToString()},FinancialTransactionDetail,\"Invalid FundraisingGroupId ({transactionDetailCsv.FundraisingGroupId}) for FinancialTransactionDetail {transactionDetailCsv.Id}. The transaction detail will be created, but the connection to the group will not.\"\r\n";
                                }
                            }
                            newFinancialTransactionImport.FinancialTransactionDetailImports.Add( newFinancialTransactionDetail );
                        }

                        financialTransactionImportList.Add( newFinancialTransactionImport );
                    }

                    completed += BulkImportFinancialTransactions( rockContext, financialTransactionImportList, existingImportedTransactionsHash, accountIdLookup, personAliasIdLookup, giverAnonymousPersonAliasId );
                    transactionsRemainingToProcess -= csvChunk.Count;
                    workingTransactionCsvList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }

            return completed;
        }

        /// <summary>
        /// Bulks the financial transaction import.
        /// </summary>
        /// <param name="rockContext">The RockContext.</param>
        /// <param name="financialTransactionImports">The financial transaction imports.</param>
        /// <param name="existingImportedTransactionsHash">A hashset of imported transaction foreign keys.</param>
        /// <param name="accountIdLookup">A guid keyed dictionary of FinancialAccount ids.</param>
        /// <param name="personAliasIdLookup">A guid keyed dictionary of PersonAlias ids.</param>
        /// <param name="giverAnonymousPersonAliasId">The anonymous giver PersonAliasId.</param>
        /// <returns></returns>
        public int BulkImportFinancialTransactions( RockContext rockContext, List<FinancialTransactionImport> financialTransactionImports, HashSet<string> existingImportedTransactionsHash, Dictionary<string, int> accountIdLookup, Dictionary<string, int> personAliasIdLookup, int giverAnonymousPersonAliasId )
        {
            var newFinancialTransactionImports = financialTransactionImports.Where( a => !existingImportedTransactionsHash.Contains( a.FinancialTransactionForeignKey ) ).ToList();
            var existingFinancialTransactionImports = financialTransactionImports.Where( a => existingImportedTransactionsHash.Contains( a.FinancialTransactionForeignKey ) ).ToList();

            var importDateTime = RockDateTime.Now;

            // Insert FinancialPaymentDetail for all the transactions first
            var financialPaymentDetailToInsert = new List<FinancialPaymentDetail>();
            foreach ( var financialTransactionImport in newFinancialTransactionImports )
            {
                var newFinancialPaymentDetail = new FinancialPaymentDetail
                {
                    CurrencyTypeValueId = financialTransactionImport.CurrencyTypeValueId,
                    ForeignKey = financialTransactionImport.FinancialTransactionForeignKey,
                    CreatedDateTime = financialTransactionImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                    ModifiedDateTime = financialTransactionImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime,
                    CreditCardTypeValueId = financialTransactionImport.CreditCardTypeValueId
                };

                financialPaymentDetailToInsert.Add( newFinancialPaymentDetail );
            }

            rockContext.BulkInsert( financialPaymentDetailToInsert );

            var financialPaymentDetailLookup = new FinancialPaymentDetailService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .Select( a => new { a.Id, a.ForeignKey } ).ToDictionary( k => k.ForeignKey, v => v.Id );

            // Prepare and Insert FinancialTransactions
            var financialTransactionsToInsert = new List<FinancialTransaction>();
            foreach ( var financialTransactionImport in newFinancialTransactionImports )
            {
                var newFinancialTransaction = new FinancialTransaction
                {
                    ForeignKey = financialTransactionImport.FinancialTransactionForeignKey,
                    BatchId = this.ImportedBatches.GetValueOrNull( financialTransactionImport.BatchForeignKey ),
                    FinancialPaymentDetailId = financialPaymentDetailLookup.GetValueOrNull( financialTransactionImport.FinancialTransactionForeignKey ),
                    Summary = financialTransactionImport.Summary,
                    TransactionCode = financialTransactionImport.TransactionCode,
                    TransactionDateTime = financialTransactionImport.TransactionDate.ToSQLSafeDate(),
                    SourceTypeValueId = financialTransactionImport.TransactionSourceValueId,
                    TransactionTypeValueId = financialTransactionImport.TransactionTypeValueId,
                    CreatedDateTime = financialTransactionImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                    ModifiedDateTime = financialTransactionImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime,
                    NonCashAssetTypeValueId = financialTransactionImport.NonCashAssetValueId,
                    ScheduledTransactionId = financialTransactionImport.ScheduledTransactionId,
                    FinancialGatewayId = financialTransactionImport.FinancialGatewayId,
                    ShowAsAnonymous = financialTransactionImport.IsAnonymous
                };

                if ( financialTransactionImport.AuthorizedPersonForeignKey.IsNotNullOrWhiteSpace() )
                {
                    newFinancialTransaction.AuthorizedPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionImport.AuthorizedPersonForeignKey );
                }

                if ( !newFinancialTransaction.AuthorizedPersonAliasId.HasValue )
                {
                    newFinancialTransaction.AuthorizedPersonAliasId = giverAnonymousPersonAliasId;
                }

                if ( financialTransactionImport.CreatedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                {
                    newFinancialTransaction.CreatedByPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionImport.CreatedByPersonForeignKey );
                }

                if ( !newFinancialTransaction.CreatedByPersonAliasId.HasValue )
                {
                    newFinancialTransaction.CreatedByPersonAliasId = ImportPersonAliasId;
                }

                if ( financialTransactionImport.ModifiedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                {
                    newFinancialTransaction.ModifiedByPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionImport.ModifiedByPersonForeignKey );
                }

                if ( !newFinancialTransaction.ModifiedByPersonAliasId.HasValue )
                {
                    newFinancialTransaction.ModifiedByPersonAliasId = ImportPersonAliasId;
                }

                financialTransactionsToInsert.Add( newFinancialTransaction );
            }

            rockContext.BulkInsert( financialTransactionsToInsert );

            var financialTransactionIdLookup = new FinancialTransactionService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .Select( a => new { a.Id, a.ForeignKey } )
                .ToList().ToDictionary( k => k.ForeignKey, v => v.Id );

            // Prepare and Insert the FinancialTransactionDetail records
            var financialTransactionDetailsToInsert = new List<FinancialTransactionDetail>();
            foreach ( var financialTransactionImport in newFinancialTransactionImports )
            {
                foreach ( var financialTransactionDetailImport in financialTransactionImport.FinancialTransactionDetailImports )
                {
                    var newFinancialTransactionDetail = new FinancialTransactionDetail
                    {
                        TransactionId = financialTransactionIdLookup[financialTransactionImport.FinancialTransactionForeignKey],
                        ForeignKey = financialTransactionDetailImport.FinancialTransactionDetailForeignKey,
                        Amount = financialTransactionDetailImport.Amount,
                        AccountId = accountIdLookup[financialTransactionDetailImport.FinancialAccountForeignKey],
                        Summary = financialTransactionDetailImport.Summary,
                        CreatedDateTime = financialTransactionDetailImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                        ModifiedDateTime = financialTransactionDetailImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime,
                        EntityTypeId = financialTransactionDetailImport.EntityTypeId,
                        EntityId = financialTransactionDetailImport.EntityId
                    };
                    if ( financialTransactionDetailImport.CreatedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                    {
                        newFinancialTransactionDetail.CreatedByPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionDetailImport.CreatedByPersonForeignKey );
                    }

                    if ( !newFinancialTransactionDetail.CreatedByPersonAliasId.HasValue )
                    {
                        newFinancialTransactionDetail.CreatedByPersonAliasId = ImportPersonAliasId;
                    }
                    if ( financialTransactionDetailImport.ModifiedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                    {
                        newFinancialTransactionDetail.ModifiedByPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionDetailImport.ModifiedByPersonForeignKey );
                    }

                    if ( !newFinancialTransactionDetail.ModifiedByPersonAliasId.HasValue )
                    {
                        newFinancialTransactionDetail.ModifiedByPersonAliasId = ImportPersonAliasId;
                    }

                    financialTransactionDetailsToInsert.Add( newFinancialTransactionDetail );
                }
            }
            rockContext.BulkInsert( financialTransactionDetailsToInsert );
            return financialTransactionsToInsert.Count();
        }
    }
}