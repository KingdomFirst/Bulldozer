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
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Drawing.Imaging;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
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
            var imageDecoderLookup = ImageCodecInfo.GetImageDecoders().ToDictionary( k => k.FormatID, v => v );
            var lookupContext = new RockContext();

            var importedTransactions = new FinancialTransactionService( lookupContext )
                .Queryable().AsNoTracking().Where( t => t.ForeignKey != null && t.ForeignKey.StartsWith( importInstanceFKPrefix + "^" ) )
                .ToDictionary( t => t.ForeignKey, t => t.Id );

            var existingTransactionImageFKs = new FinancialTransactionImageService( lookupContext ).Queryable()
                .Where( ti => ti.ForeignKey != null && ti.ForeignKey.StartsWith( importInstanceFKPrefix + "^" ) )
                .ToDictionary( ti => ti.ForeignKey, ti => ti.ForeignKey );

            var existingBinaryFileFKs = new List<string>();
            var existingBinaryFileDict = LoadBinaryFileDict( lookupContext, importInstanceFKPrefix, out existingBinaryFileFKs );
            
            var errors = string.Empty;

            var totalEntries = folder.Entries.Count;
            var percentage = ( totalEntries - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying transaction images import ({0:N0} found.)", totalEntries ) );

            ReportProgress( 0, $"Processing {totalEntries} transaction image files" );

            // Slice import files into chunks and process
            var workingFileImportList = folder.Entries.OrderBy( f => f.Name ).ToList();
            var filesRemainingToProcess = totalEntries;
            var completedItems = 0;

            while ( filesRemainingToProcess > 0 )
            {
                if ( completedItems > 0 && completedItems % ( chunkSize * 10 ) < 1 )
                {
                    var percentComplete = completedItems / percentage;
                    ReportProgress( percentComplete, string.Format( "{0:N0} transaction image files imported ({1}% complete).", completedItems, percentComplete ) );
                }

                if ( completedItems % chunkSize < 1 )
                {
                    var fileChunk = workingFileImportList.Take( Math.Min( chunkSize, workingFileImportList.Count ) ).ToList();
                    completedItems += ProcessImages( fileChunk, lookupContext, importedTransactions, existingBinaryFileDict, existingBinaryFileFKs, imageDecoderLookup, transactionImageType, existingTransactionImageFKs, importInstanceFKPrefix, errors );

                    if ( errors.IsNotNullOrWhiteSpace() )
                    {
                        LogException( null, errors, hasMultipleErrors: true );
                        errors = string.Empty;
                    }

                    filesRemainingToProcess -= fileChunk.Count;
                    workingFileImportList.RemoveRange( 0, fileChunk.Count );
                    ReportPartialProgress();
                }
            }

            return completedItems;
        }

        /// <summary>
        /// Create new binary files and transaction images from imported files.
        /// </summary>
        /// <param name="importFiles">The list of import files to process</param>
        /// <param name="rockContext">The RockContext to use</param>
        /// <param name="importedTransactions">The dictionary of imported transactions</param>
        /// <param name="existingBinaryFileDict">The dictionary of existing BinaryFiles</param>
        /// <param name="existingBinaryFileFKs">The list of existing BinaryFile ForeignKeys</param>
        /// <param name="imageDecoderLookup">The dictionary of image codec decoder information</param>
        /// <param name="transactionImageType">The Transaction Image BinaryFileType object</param>
        /// <param name="existingTransactionImageFKs">The dictionary of existing transaction image ForeignKey values</param>
        /// <param name="importInstanceFKPrefix">The import prefix to use for entity ForeignKeys</param>
        /// <param name="errors">The string containing error messages</param>
        /// <returns></returns>
        public int ProcessImages( List<ZipArchiveEntry> importFiles, RockContext rockContext, Dictionary<string, int> importedTransactions, Dictionary<Guid, int> existingBinaryFileDict, Dictionary<string,string> existingBinaryFileFKs, Dictionary<Guid, ImageCodecInfo> imageDecoderLookup, BinaryFileType transactionImageType, Dictionary<string, string> existingTransactionImageFKs, string importInstanceFKPrefix, string errors )
        {
            var newBinaryFiles = new List<Rock.Model.BinaryFile>();
            var newFileList = new List<TransactionImageKeys>();
            foreach ( var file in importFiles )
            {
                var fileExtension = Path.GetExtension( file.Name );
                if ( FileTypeBlackList.Contains( fileExtension ) )
                {
                    LogException( "Binary File Import", string.Format( "{0} filetype not allowed ({1})", fileExtension, file.Name ) );
                    continue;
                }

                var nameWithoutExtension = Path.GetFileNameWithoutExtension( file.Name );

                var foreignTransactionId = nameWithoutExtension.AsIntegerOrNull();
                var foreignKey = $"{importInstanceFKPrefix}^{foreignTransactionId}";
                var transactionId = importedTransactions.GetValueOrNull( foreignKey );
                if ( !transactionId.HasValue )
                {
                    errors += $"{DateTime.Now}, Binary File Import, Foreign Transaction Id '{foreignTransactionId}' not found in Rock. File '{file.Name}' was not imported.\r\n";
                    continue;
                }
                else
                {

                    if ( existingBinaryFileFKs.Any( fk => fk == foreignKey ) )
                    {
                        errors += $"{DateTime.Now}, Binary File Import, Binary file with ForeignKey '{foreignKey}' already exists. Filename '{file.Name}' was not imported.\r\n";
                        continue;
                    }

                    var rockFile = new Rock.Model.BinaryFile
                    {
                        IsSystem = false,
                        IsTemporary = false,
                        BinaryFileTypeId = transactionImageType.Id,
                        CreatedDateTime = file.LastWriteTime.DateTime,
                        Description = string.Format( "Imported as {0}", file.Name ),
                        Guid = Guid.NewGuid(),
                        ForeignKey = foreignKey
                    };

                    var newTransactionImageKeys = new TransactionImageKeys();
                    newTransactionImageKeys.TransactionId = transactionId.Value;
                    newTransactionImageKeys.TransactionImageForeignKey = foreignKey;

                    // use base stream instead of file stream to keep the byte[]
                    using ( var fileContent = new StreamReader( file.Open() ) )
                    {
                        var imageBytes = fileContent.BaseStream.ReadBytesToEnd();
                        newTransactionImageKeys.ImageData = Convert.ToBase64String( imageBytes );
                        rockFile.FileSize = imageBytes.Length;
                    }

                    // Figure out the mimetype based on the file Stream since we know Arena lies sometimes. 
                    // If we successfully extract a mimetype from the Stream we use it and change the file extension to match.
                    using ( var image = new System.Drawing.Bitmap( file.Open() ) )
                    {
                        var imageDecoder = imageDecoderLookup.GetValueOrNull( image.RawFormat.Guid );
                        var mimeType = imageDecoder?.MimeType;
                        var extension = imageDecoder?.FilenameExtension?.Split( ';' ).FirstOrDefault()?.Replace( "*", string.Empty ).ToLower();
                        if ( mimeType != null )
                        {
                            rockFile.MimeType = mimeType;
                            if ( extension != null )
                            {
                                rockFile.FileName = string.Format( "{0}{1}", nameWithoutExtension, extension );
                            }
                            else
                            {
                                rockFile.FileName = file.Name;
                            }
                        }
                        else
                        {
                            rockFile.MimeType = GetMIMEType( file.Name );
                            rockFile.FileName = file.Name;
                        }
                    }

                    rockFile.SetStorageEntityTypeId( transactionImageType.StorageEntityTypeId );

                    if ( transactionImageType.AttributeValues.Any() )
                    {
                        rockFile.StorageEntitySettings = transactionImageType.AttributeValues
                            .ToDictionary( a => a.Key, v => v.Value.Value ).ToJson();
                    }

                    if ( rockFile.StorageProvider == null )
                    {
                        errors += $"{DateTime.Now}, Binary File Import, Could not load storage provider for filename '{file.Name}'. File was not imported.\r\n";
                        continue;
                    }

                    rockFile.StorageProvider.SaveContent( rockFile );
                    rockFile.Path = rockFile.StorageProvider.GetPath( rockFile );

                    newTransactionImageKeys.File = rockFile;

                    newBinaryFiles.Add( rockFile );
                    newFileList.Add( newTransactionImageKeys );
                    existingBinaryFileFKs.Add( foreignKey );
                }
            }

            rockContext.BulkInsert( newBinaryFiles );

            existingBinaryFileDict = LoadBinaryFileDict( rockContext, importInstanceFKPrefix, out existingBinaryFileFKs );

            var newBinaryFileDatas = new List<Rock.Model.BinaryFileData>();
            var newTransactionImages = new List<FinancialTransactionImage>();
            foreach ( var entry in newFileList.Where( f => f.File != null && f.File.BinaryFileTypeId != null ) )
            {
                var binaryFile = existingBinaryFileDict.GetValueOrNull( entry.File.Guid );

                // Null file should not happen, but handling and reporting it just in case.
                if ( binaryFile == null )
                {
                    errors += $"{DateTime.Now}, Binary File Import, No binary file found for transaction image '{entry.TransactionImageForeignKey}'. No new binary file was created for Filename '{entry.File.FileName}'.\r\n";
                    continue;
                }

                var newBinaryFileData = new BinaryFileData()
                {
                    Id = binaryFile.Id,
                    Content = Convert.FromBase64String( entry.ImageData ),
                    CreatedDateTime = RockDateTime.Now,
                    ModifiedDateTime = RockDateTime.Now,
                    ForeignKey = entry.TransactionImageForeignKey
                };

                newBinaryFileDatas.Add( newBinaryFileData );

                var existingTransactionImageFK = existingTransactionImageFKs.GetValueOrNull( entry.TransactionImageForeignKey );
                if ( existingTransactionImageFK.IsNotNullOrWhiteSpace() )
                {
                    errors += $"{DateTime.Now}, Binary File Import, Financial Transaction Image with ForeignKey '{entry.TransactionImageForeignKey}' already exists. No new Financial Transaction Image was created for Filename '{entry.File.FileName}'.\r\n";
                    continue;
                }

                var transactionImage = new FinancialTransactionImage
                {
                    TransactionId = entry.TransactionId,
                    BinaryFile = binaryFile,
                    Order = 0,
                    ForeignKey = entry.TransactionImageForeignKey
                };

                var isValid = transactionImage.IsValid;
                if ( !isValid )
                {
                    errors += $"{DateTime.Now}, Binary File Import, An error was encountered when trying to create the Financial Transaction Image for filename {entry.File.FileName}': {transactionImage.ValidationResults.Select( a => a.ErrorMessage ).ToList().AsDelimited( "\r\n" )}\r\n";
                    continue;
                }

                newTransactionImages.Add( transactionImage );
                existingTransactionImageFKs.Add( transactionImage.ForeignKey, transactionImage.ForeignKey );
            }

            rockContext.BulkInsert( newBinaryFileDatas );

            if ( newTransactionImages.Any() )
            {
                var transactionImageService = new FinancialTransactionImageService( rockContext );
                transactionImageService.AddRange( newTransactionImages );
                rockContext.SaveChanges();

                existingTransactionImageList = LoadTransactionImageList( rockContext, importInstanceFKPrefix );

                // Set security on binary files to financial transaction image

                var transactionImageEntityTypeId = EntityTypeCache.GetId( Rock.SystemGuid.EntityType.FINANCIAL_TRANSACTION_IMAGE );
                var transactionImageInfo = existingTransactionImageList.ToDictionary( d => d.Guid, d => d.Id );
                foreach ( var image in newTransactionImages )
                {
                    var binaryFile = existingBinaryFileDict.GetValueOrNull( image.BinaryFile.Guid );
                    binaryFile.ParentEntityTypeId = transactionImageEntityTypeId;
                    binaryFile.ParentEntityId = transactionImageInfo.GetValueOrNull( image.Guid );
                }

                rockContext.SaveChanges();
            }
            return importFiles.Count;
        }

        {
        }
    }
}