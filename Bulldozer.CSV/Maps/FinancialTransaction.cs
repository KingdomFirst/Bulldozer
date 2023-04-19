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
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using static Bulldozer.Utility.Extensions;

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
            if ( this.ImportedBatches == null )
            {
                LoadImportedBatches( rockContext );
            }
            if ( this.ImportedAccounts == null )
            {
                LoadImportedAccounts( rockContext );
            }
            ReportProgress( 0, string.Format( "Begin processing {0} FinancialTransaction Records...", this.FinancialTransactionCsvList.Count ) );


            int giverAnonymousPersonAliasId = new PersonService( rockContext ).GetOrCreateAnonymousGiverPerson().Aliases.FirstOrDefault().Id;
            var existingImportedTransactions = new FinancialTransactionService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) );
            var existingImportedTransactionsHash = new HashSet<string>( existingImportedTransactions.Select( a => a.ForeignKey ).ToList() );
            var personAliasIdLookup = ImportedPeopleKeys.ToDictionary( k => k.Key, v => v.Value.PersonAliasId );

            var accountIdLookup = ImportedAccounts.ToDictionary( k => k.Key, v => v.Value.Id );

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
                    var errors = string.Empty;
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
                            NonCashAssetValueId = this.NonCashAssetTypeValues[financialTransactionCsv.NonCashAssetType].Id,
                            TransactionTypeValueId = this.TransactionTypeValues[financialTransactionCsv.TransactionType.ToString()].Id,
                            CreatedByPersonForeignKey = $"{ImportInstanceFKPrefix}^{financialTransactionCsv.CreatedByPersonId}",
                            CreatedDateTime = financialTransactionCsv.CreatedDateTime.ToSQLSafeDate(),
                            ModifiedByPersonForeignKey = $"{ImportInstanceFKPrefix}^{financialTransactionCsv.ModifiedByPersonId}",
                            ModifiedDateTime = financialTransactionCsv.ModifiedDateTime.ToSQLSafeDate(),
                            FinancialTransactionDetailImports = new List<FinancialTransactionDetailImport>(),
                        };

                        if ( financialTransactionCsv.TransactionSource.IsNotNullOrWhiteSpace() )
                        {
                            newFinancialTransactionImport.TransactionSourceValueId = this.TransactionSourceTypeValues[financialTransactionCsv.TransactionSource].Id;
                        }
                        else
                        {
                            // set default source to onsite, exceptions listed below
                            newFinancialTransactionImport.TransactionSourceValueId = this.TransactionSourceTypeValues["On-Site"].Id;
                        }

                        foreach ( var transactionDetailCsv in financialTransactionCsv.FinancialTransactionDetails )
                        {
                            var newFinancialTransactionDetail = new FinancialTransactionDetailImport()
                            {
                                FinancialAccountForeignKey = $"{ImportInstanceFKPrefix}^{transactionDetailCsv.AccountId}",
                                Amount = transactionDetailCsv.Amount,
                                CreatedByPersonForeignKey = $"{ImportInstanceFKPrefix}^{transactionDetailCsv.CreatedByPersonId}",
                                CreatedDateTime = transactionDetailCsv.CreatedDateTime.ToSQLSafeDate(),
                                FinancialTransactionDetailForeignKey = transactionDetailCsv.Id,
                                ModifiedByPersonForeignKey = $"{ImportInstanceFKPrefix}^{transactionDetailCsv.ModifiedByPersonId}",
                                ModifiedDateTime = transactionDetailCsv.ModifiedDateTime.ToSQLSafeDate(),
                                Summary = transactionDetailCsv.Summary
                            };
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

            return completed;
        }

        /// <summary>
        /// Bulks the financial transaction import.
        /// </summary>
        /// <param name="financialTransactionImports">The financial transaction imports.</param>
        /// <returns></returns>
        public int BulkImportFinancialTransactions( RockContext rockContext, List<FinancialTransactionImport> financialTransactionImports, HashSet<string> existingImportedTransactionsHash, Dictionary<string, int> accountIdLookup, Dictionary<string, int> personAliasIdLookup, int giverAnonymousPersonAliasId )
        {

            var newFinancialTransactionImports = financialTransactionImports.Where( a => !existingImportedTransactionsHash.Contains( a.FinancialTransactionForeignKey ) ).ToList();
            var existingFinancialTransactionImports = financialTransactionImports.Where( a => existingImportedTransactionsHash.Contains( a.FinancialTransactionForeignKey ) ).ToList();

            var importDateTime = RockDateTime.Now;

            // Insert FinancialPaymentDetail for all the transactions first
            var financialPaymentDetailToInsert = new List<FinancialPaymentDetail>( newFinancialTransactionImports.Count );
            foreach ( var financialTransactionImport in newFinancialTransactionImports )
            {
                var newFinancialPaymentDetail = new FinancialPaymentDetail();
                newFinancialPaymentDetail.CurrencyTypeValueId = financialTransactionImport.CurrencyTypeValueId;
                newFinancialPaymentDetail.ForeignKey = financialTransactionImport.FinancialTransactionForeignKey;
                newFinancialPaymentDetail.CreatedDateTime = financialTransactionImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime;
                newFinancialPaymentDetail.ModifiedDateTime = financialTransactionImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime;

                financialPaymentDetailToInsert.Add( newFinancialPaymentDetail );
            }

            rockContext.BulkInsert( financialPaymentDetailToInsert );

            var financialPaymentDetailLookup = new FinancialPaymentDetailService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .Select( a => new { a.Id, a.ForeignKey } ).ToDictionary( k => k.ForeignKey, v => v.Id );

            // Prepare and Insert FinancialTransactions
            var financialTransactionsToInsert = new List<FinancialTransaction>();
            foreach ( var financialTransactionImport in newFinancialTransactionImports )
            {
                var newFinancialTransaction = new FinancialTransaction();
                newFinancialTransaction.ForeignKey = financialTransactionImport.FinancialTransactionForeignKey;

                if ( financialTransactionImport.AuthorizedPersonForeignKey.IsNotNullOrWhiteSpace() )
                {
                    newFinancialTransaction.AuthorizedPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionImport.AuthorizedPersonForeignKey );
                }

                if ( !newFinancialTransaction.AuthorizedPersonAliasId.HasValue )
                {
                    newFinancialTransaction.AuthorizedPersonAliasId = giverAnonymousPersonAliasId;
                }

                newFinancialTransaction.BatchId = this.ImportedBatches.GetValueOrNull( financialTransactionImport.BatchForeignKey );
                newFinancialTransaction.FinancialPaymentDetailId = financialPaymentDetailLookup.GetValueOrNull( financialTransactionImport.FinancialTransactionForeignKey );

                newFinancialTransaction.Summary = financialTransactionImport.Summary;
                newFinancialTransaction.TransactionCode = financialTransactionImport.TransactionCode;
                newFinancialTransaction.TransactionDateTime = financialTransactionImport.TransactionDate.ToSQLSafeDate();
                newFinancialTransaction.SourceTypeValueId = financialTransactionImport.TransactionSourceValueId;
                newFinancialTransaction.TransactionTypeValueId = financialTransactionImport.TransactionTypeValueId;
                newFinancialTransaction.CreatedDateTime = financialTransactionImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime;
                newFinancialTransaction.ModifiedDateTime = financialTransactionImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime;
                newFinancialTransaction.NonCashAssetTypeValueId = financialTransactionImport.NonCashAssetValueId;

                if ( financialTransactionImport.CreatedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                {
                    newFinancialTransaction.CreatedByPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionImport.CreatedByPersonForeignKey );
                }
                if ( financialTransactionImport.ModifiedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                {
                    newFinancialTransaction.ModifiedByPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionImport.ModifiedByPersonForeignKey );
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
                        ModifiedDateTime = financialTransactionDetailImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime
                    };
                    if ( financialTransactionDetailImport.CreatedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                    {
                        newFinancialTransactionDetail.CreatedByPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionDetailImport.CreatedByPersonForeignKey );
                    }
                    if ( financialTransactionDetailImport.ModifiedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                    {
                        newFinancialTransactionDetail.ModifiedByPersonAliasId = personAliasIdLookup.GetValueOrNull( financialTransactionDetailImport.ModifiedByPersonForeignKey );
                    }
                    financialTransactionDetailsToInsert.Add( newFinancialTransactionDetail );
                }
            }
            rockContext.BulkInsert( financialTransactionDetailsToInsert );
            return financialTransactionsToInsert.Count();
        }
    }
}