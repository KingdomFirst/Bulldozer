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
using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Storage;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.BinaryFile
{
    /// <summary>
    /// Maps Benevolence Request Documents
    /// </summary>
    public class BenevolenceRequestDocuments : BinaryFileComponent, IBinaryFile
    {
        /// <summary>
        /// Maps the specified folder.
        /// </summary>
        /// <param name="folder">The folder.</param>
        /// <param name="requestDocumentType">The benevolence request document file type.</param>
        public int Map( ZipArchive folder, BinaryFileType requestDocumentType, int chunkSize, string importInstanceFKPrefix )
        {
            var lookupContext = new RockContext();

            var emptyJsonObject = "{}";
            var newFileList = new Dictionary<KeyValuePair<int, int>, Rock.Model.BinaryFile>();
            var benevolenceRequestService = new BenevolenceRequestService( lookupContext );
            var importedRequests = benevolenceRequestService
                .Queryable().AsNoTracking().Where( t => t.ForeignKey != null && t.ForeignKey.StartsWith( importInstanceFKPrefix + "^" ) )
                .ToDictionary( t =>t.ForeignKey, t => t.Id );
            var importedRequestDocuments = new BenevolenceRequestDocumentService( lookupContext )
                .Queryable().AsNoTracking().Where( t => t.ForeignKey != null && t.ForeignKey.StartsWith( importInstanceFKPrefix + "^" ) )
                .ToDictionary( t => t.ForeignKey, t => t.Id );

            ProviderComponent storageProvider;
            if ( requestDocumentType.StorageEntityTypeId == DatabaseProvider.EntityType.Id )
            {
                storageProvider = ( ProviderComponent ) DatabaseProvider;
            }
            else if ( requestDocumentType.StorageEntityTypeId == AzureBlobStorageProvider.EntityType.Id )
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
            ReportProgress( 0, string.Format( "Verifying benevolence request documents import ({0:N0} found.)", totalEntries ) );

            foreach ( var file in folder.Entries )
            {
                var fileExtension = Path.GetExtension( file.Name );
                if ( FileTypeBlackList.Contains( fileExtension ) )
                {
                    LogException( "Binary File Import", string.Format( "{0} filetype not allowed ({1})", fileExtension, file.Name ) );
                    continue;
                }

                var nameWithoutExtension = file.Name.ReplaceLastOccurrence( fileExtension, string.Empty );
                var parsedFileName = nameWithoutExtension.Split( '_' ).ToList();

                //
                // Benevolence Request docs should follow this pattern:
                // 0. Request ForeignId
                // 1. FileName
                // 2. Doc Id

                var foreignBenevolenceRequestId = parsedFileName[0].AsType<int?>();

                // Make sure the Benevolence Request exists
                var benevolenceRequestId = importedRequests.GetValueOrNull( string.Format( "{0}^{1}", importInstanceFKPrefix, foreignBenevolenceRequestId ) );
                if ( benevolenceRequestId.HasValue )
                {
                    var benevolenceRequest = benevolenceRequestService.Queryable().AsNoTracking().FirstOrDefault( r => r.Id == benevolenceRequestId.Value );
                    var documentForeignId = -1;
                    var fileName = string.Empty;
                    if ( parsedFileName.Count() >= 3 )
                    {
                        documentForeignId = parsedFileName.LastOrDefault().AsInteger();

                        // If document foreignId is provided, make sure it doesn't already exist
                        var requestDocumentId = importedRequestDocuments.GetValueOrNull( string.Format( "{0}^{1}", importInstanceFKPrefix, documentForeignId ) );
                        if ( requestDocumentId.HasValue )
                        {
                            continue;
                        }

                        // Extract filename
                        parsedFileName.RemoveAt( parsedFileName.Count() - 1 );    // Remove Doc Id from end
                        parsedFileName.RemoveAt( 0 );   // Remove Request ForeignId from beginning
                        fileName = string.Join( "_", parsedFileName );
                    }
                    else
                    {
                        var filename = file.Name.ReplaceLastOccurrence( fileExtension, string.Empty );
                    }

                    // Create the binary file
                    var rockFile = new Rock.Model.BinaryFile
                    {
                        IsSystem = false,
                        IsTemporary = false,
                        MimeType = GetMIMEType( file.Name ),
                        BinaryFileTypeId = requestDocumentType.Id,
                        FileName = fileName,
                        CreatedDateTime = file.LastWriteTime.DateTime,
                        ModifiedDateTime = file.LastWriteTime.DateTime,
                        CreatedByPersonAliasId = ImportPersonAliasId
                    };

                    rockFile.SetStorageEntityTypeId( requestDocumentType.StorageEntityTypeId );
                    rockFile.StorageEntitySettings = emptyJsonObject;

                    if ( requestDocumentType.AttributeValues.Any() )
                    {
                        rockFile.StorageEntitySettings = requestDocumentType.AttributeValues
                            .ToDictionary( a => a.Key, v => v.Value.Value ).ToJson();
                    }

                    // use base stream instead of file stream to keep the byte[]
                    // NOTE: if byte[] converts to a string it will corrupt the stream
                    using ( var fileContent = new StreamReader( file.Open() ) )
                    {
                        rockFile.ContentStream = new MemoryStream( fileContent.BaseStream.ReadBytesToEnd() );
                    }

                    //  add this document file to the Rock transaction
                    newFileList.Add( new KeyValuePair<int, int>( benevolenceRequestId.Value, documentForeignId ), rockFile );

                    completedItems++;
                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} benevolence document files imported ({1}% complete).", completedItems, percentComplete ) );
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

            ReportProgress( 100, string.Format( "Finished document import: {0:N0} benevolence documents imported.", completedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the files.
        /// </summary>
        /// <param name="newFileList">The new file list.</param>
        /// <param name="storageProvider">The storage provider.</param>
        private static void SaveFiles( Dictionary<KeyValuePair<int, int>, Rock.Model.BinaryFile> newFileList, ProviderComponent storageProvider )
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

                    // associate the document with the correct BenevolenceRequest
                    var benevolenceDocument = new BenevolenceRequestDocument
                    {
                        BenevolenceRequestId = entry.Key.Key,
                        BinaryFileId = entry.Value.Id,
                        Order = 0
                    };

                    if ( entry.Key.Value > 0 )
                    {
                        benevolenceDocument.ForeignKey = entry.Key.Value.ToString();
                        benevolenceDocument.ForeignId = entry.Key.Value;
                    }

                    rockContext.BenevolenceRequests.FirstOrDefault( r => r.Id == entry.Key.Key )
                        .Documents.Add( benevolenceDocument );
                }

                rockContext.SaveChanges( DisableAuditing );
            } );
        }
    }
}