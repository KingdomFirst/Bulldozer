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
using System.Data.Entity;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Storage;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.BinaryFile
{
    /// <summary>
    /// Maps Transaction Images
    /// </summary>
    public class TransactionImage : BinaryFileComponent, IBinaryFile
    {
        /// <summary>
        /// Maps the specified folder.
        /// </summary>
        /// <param name="folder">The ZipArchive containing the folder of binary files</param>
        /// <param name="transactionImageType">Type of the transaction image file</param>
        /// <param name="chunkSize">The chunk size to use for processing files</param>
        /// <param name="importInstanceFKPrefix">The import prefix to use for entity ForeignKeys</param>
        public int Map( ZipArchive folder, BinaryFileType transactionImageType, int chunkSize, string importInstanceFKPrefix )
        {
            var lookupContext = new RockContext();

            var emptyJsonObject = "{}";
            var newFileList = new Dictionary<int, Rock.Model.BinaryFile>();
            var importedTransactions = new FinancialTransactionService( lookupContext )
                .Queryable().AsNoTracking().Where( t => t.ForeignKey != null && t.ForeignKey.StartsWith( importInstanceFKPrefix + "^" ) )
                .ToDictionary( t => t.ForeignKey, t => t.Id );

            ProviderComponent storageProvider;
            if ( transactionImageType.StorageEntityTypeId == DatabaseProvider.EntityType.Id )
            {
                storageProvider = ( ProviderComponent ) DatabaseProvider;
            }
            else if ( transactionImageType.StorageEntityTypeId == AzureBlobStorageProvider.EntityType.Id )
            {
                storageProvider = ( ProviderComponent ) AzureBlobStorageProvider;
            }
            else
            {
                storageProvider = ( ProviderComponent ) FileSystemProvider;
            }

            var completedItems = 0;
            var totalEntries = folder.Entries.Count;
            var percentage = ( totalEntries - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying transaction images import ({0:N0} found.)", totalEntries ) );

            foreach ( var file in folder.Entries )
            {
                var fileExtension = Path.GetExtension( file.Name );
                if ( FileTypeBlackList.Contains( fileExtension ) )
                {
                    LogException( "Binary File Import", string.Format( "{0} filetype not allowed ({1})", fileExtension, file.Name ) );
                    continue;
                }

                var foreignTransactionId = Path.GetFileNameWithoutExtension( file.Name ).AsIntegerOrNull();
                var transactionId = importedTransactions.GetValueOrNull( $"{importInstanceFKPrefix}^{foreignTransactionId}" );
                if ( transactionId.HasValue )
                {
                    var rockFile = new Rock.Model.BinaryFile
                    {
                        IsSystem = false,
                        IsTemporary = false,
                        FileName = file.Name,
                        BinaryFileTypeId = transactionImageType.Id,
                        CreatedDateTime = file.LastWriteTime.DateTime,
                        MimeType = GetMIMEType( file.Name ),
                        Description = string.Format( "Imported as {0}", file.Name ),
                        ForeignKey = $"{importInstanceFKPrefix}^{foreignTransactionId}"
                    };

                    rockFile.SetStorageEntityTypeId( transactionImageType.StorageEntityTypeId );
                    rockFile.StorageEntitySettings = emptyJsonObject;

                    if ( transactionImageType.AttributeValues.Any() )
                    {
                        rockFile.StorageEntitySettings = transactionImageType.AttributeValues
                            .ToDictionary( a => a.Key, v => v.Value.Value ).ToJson();
                    }

                    // use base stream instead of file stream to keep the byte[]
                    // NOTE: if byte[] converts to a string it will corrupt the stream
                    using ( var fileContent = new StreamReader( file.Open() ) )
                    {
                        rockFile.ContentStream = new MemoryStream( fileContent.BaseStream.ReadBytesToEnd() );
                    }

                    // add this transaction image to the Rock transaction
                    newFileList.Add( transactionId.Value, rockFile );

                    completedItems++;
                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} transaction image files imported ({1}% complete).", completedItems, percentComplete ) );
                    }

                    if ( completedItems % chunkSize < 1 )
                    {
                        SaveFiles( newFileList, storageProvider );

                        // Reset list
                        newFileList.Clear();
                        ReportPartialProgress();
                    }
                }
            }

            if ( newFileList.Any() )
            {
                SaveFiles( newFileList, storageProvider );
            }

            ReportProgress( 100, string.Format( "Finished images import: {0:N0} transaction images imported.", completedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the files.
        /// </summary>
        /// <param name="newFileList">The new file list.</param>
        /// <param name="storageProvider">The storage provider.</param>
        private static void SaveFiles( Dictionary<int, Rock.Model.BinaryFile> newFileList, ProviderComponent storageProvider )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.BinaryFiles.AddRange( newFileList.Values );
                rockContext.SaveChanges( DisableAuditing );

                foreach ( var entry in newFileList )
                {
                    if ( entry.Value != null )
                    {
                        if ( storageProvider != null )
                        {
                            storageProvider.SaveContent( entry.Value );
                            entry.Value.Path = storageProvider.GetPath( entry.Value );
                        }
                        else
                        {
                            LogException( "Binary File Import", string.Format( "Could not load provider {0}.", storageProvider.ToString() ) );
                        }
                    }

                    // associate the image with the right transaction
                    var transactionImage = new FinancialTransactionImage
                    {
                        TransactionId = entry.Key,
                        BinaryFileId = entry.Value.Id,
                        Order = 0,
                        ForeignKey = entry.Value.ForeignKey
                    };

                    rockContext.FinancialTransactions.FirstOrDefault( t => t.Id == entry.Key )
                        .Images.Add( transactionImage );
                }

                rockContext.SaveChanges( DisableAuditing );
            } );
        }
    }
}