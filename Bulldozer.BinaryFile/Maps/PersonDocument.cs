// <copyright>
// Copyright 2025 by Kingdom First Solutions
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
using System.IO;
using System.IO.Compression;
using System.Linq;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Storage;
using Rock.Web.Cache;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.BinaryFile
{
    /// <summary>
    /// Maps Person Documents
    /// </summary>
    public class PersonDocument : BinaryFileComponent, IBinaryFile
    {
        /// <summary>
        /// Maps the specified binary folder.
        /// </summary>
        /// <param name="folder">The ZipArchive containing the folder of binary files</param>
        /// <param name="documentFileType">Type of the ministry file</param>
        /// <param name="chunkSize">The chunk size to use for processing files</param>
        /// <param name="importInstanceFKPrefix">The import prefix to use for entity ForeignKeys</param>
        public int Map( ZipArchive folder, BinaryFileType documentFileType, int chunkSize, string importInstanceFKPrefix )
        {
            var lookupContext = new RockContext();
            var personEntityTypeId = EntityTypeCache.GetId<Person>();
            var personDocBinaryFileType = new BinaryFileTypeService( lookupContext ).Get( "2c0a9da7-85b5-4d30-8c8c-638c3902b711".AsGuid() );
            var documentTypeService = new DocumentTypeService( lookupContext );

            var existingDocTypeDict = documentTypeService.Queryable()
                .Where( dt => dt.EntityTypeId == personEntityTypeId && dt.BinaryFileTypeId == personDocBinaryFileType.Id )
                .ToDictionary( dt => dt.Name.Replace( " ", string.Empty ), dt => dt );

            var existingDocumentList = LoadDocumentList( lookupContext, importInstanceFKPrefix );

            var existingBinaryFileFKs = new List<string>();
            var existingBinaryFileDict = LoadBinaryFileDict( lookupContext, importInstanceFKPrefix, out existingBinaryFileFKs );

            var errors = string.Empty;

            var totalRows = folder.Entries.Count;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying person document import ({0:N0} found)", totalRows ) );

            // Create any new document types needed first
            var importDocTypes = folder.Entries.Select( e => e.Name.Split( '_' )[1] ).Distinct().ToList(); // DocumentType from import file name
            var newDocTypes = new List<DocumentType>();
            foreach ( var docType in importDocTypes )
            {
                var docTypeTrimmed = docType.Replace( " ", string.Empty );
                var documentType = existingDocTypeDict.GetValueOrNull( docTypeTrimmed );
                if ( documentType == null )
                {
                    documentType = new DocumentType
                    {
                        Name = docType,
                        Guid = Guid.NewGuid(),
                        EntityTypeId = personEntityTypeId.Value,
                        BinaryFileTypeId = personDocBinaryFileType.Id,
                        IsSystem = false,
                        UserSelectable = true,
                        CreatedDateTime = RockDateTime.Now,
                        ModifiedDateTime = RockDateTime.Now,
                        ForeignKey = $"{importInstanceFKPrefix}^{docTypeTrimmed}",
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ModifiedByPersonAliasId = ImportPersonAliasId
                    };
                    newDocTypes.Add( documentType );
                    existingDocTypeDict.Add( docTypeTrimmed, documentType );
                }
            }
            if ( newDocTypes.Any() )
            {
                ReportProgress( 0, $"Creating {newDocTypes.Count} new Document Type(s)." );
                lookupContext.BulkInsert( newDocTypes );

                existingDocTypeDict = documentTypeService.Queryable()
                    .Where( dt => dt.EntityTypeId == personEntityTypeId && dt.BinaryFileTypeId == personDocBinaryFileType.Id )
                    .ToDictionary( dt => dt.Name.Replace( " ", string.Empty ), dt => dt );
            }

            ReportProgress( 0, $"Processing {totalRows} person document files" );

            // Slice import files into chunks and process
            var workingFileImportList = folder.Entries.OrderBy( f => f.Name ).ToList();
            var filesRemainingToProcess = workingFileImportList.Count();
            var completedItems = 0;

            while ( filesRemainingToProcess > 0 )
            {
                if ( completedItems > 0 && completedItems % ( chunkSize * 10 ) < 1 )
                {
                    var percentComplete = completedItems / percentage;
                    ReportProgress( percentComplete, string.Format( "{0:N0} ministry document files imported ({1}% complete).", completedItems, percentComplete ) );
                }

                if ( completedItems % chunkSize < 1 )
                {
                    var fileChunk = workingFileImportList.Take( Math.Min( chunkSize, workingFileImportList.Count ) ).ToList();
                    completedItems += ProcessFiles( fileChunk, lookupContext, existingDocTypeDict, existingBinaryFileDict, existingDocumentList, existingBinaryFileFKs, personDocBinaryFileType, importInstanceFKPrefix, errors );
                    
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

        private List<Document> LoadDocumentList( RockContext lookupContext, string importInstanceFKPrefix )
        {
            return new DocumentService( lookupContext ).Queryable()
                            .Where( d => d.ForeignKey != null && d.ForeignKey.StartsWith( importInstanceFKPrefix + "^" ) )
                            .ToList();
        }

        private Dictionary<Guid, Rock.Model.BinaryFile> LoadBinaryFileDict( RockContext lookupContext, string importInstanceFKPrefix, out List<string> existingBinaryFileFKs )
        {
            var binaryDict = new BinaryFileService( lookupContext ).Queryable()
                .Where( f => f.ForeignKey != null && f.ForeignKey.StartsWith( importInstanceFKPrefix + "^" ) )
                .ToDictionary( f => f.Guid, f => f );

            existingBinaryFileFKs = binaryDict.Values
                            .Select( f => f.ForeignKey )
                            .Distinct()
                            .ToList();
            return binaryDict;
        }

        /// <summary>
        /// Create new binary files and documents from imported files.
        /// </summary>
        /// <param name="importFiles">The list of import files to process</param>
        /// <param name="rockContext">The RockContext to use</param>
        /// <param name="existingDocTypeDict">The dictionary of existing DocumentTypes</param>
        /// <param name="existingBinaryFileDict">The dictionary of existing BinaryFiles</param>
        /// <param name="existingDocumentList">The List of existing Documents</param>
        /// <param name="existingBinaryFileFKs">The List of existing BinaryFile ForeignKeys</param>
        /// <param name="personDocBinaryFileType">The Person BinaryFileType object</param>
        /// <param name="importInstanceFKPrefix">The import prefix to use for entity ForeignKeys</param>
        /// <param name="errors">The string containing error messages</param>
        /// <returns></returns>
        public int ProcessFiles( List<ZipArchiveEntry> importFiles, RockContext rockContext, Dictionary<string, DocumentType> existingDocTypeDict, Dictionary<Guid, Rock.Model.BinaryFile> existingBinaryFileDict, List<Document> existingDocumentList, List<string> existingBinaryFileFKs, BinaryFileType personDocBinaryFileType, string importInstanceFKPrefix, string errors )
        {
            var newFileList = new List<DocumentKeys>();
            var newBinaryFiles = new List<Rock.Model.BinaryFile>();
            foreach ( var file in importFiles )
            {
                var fileExtension = Path.GetExtension( file.Name );
                if ( FileTypeBlackList.Contains( fileExtension ) )
                {
                    errors += $"{DateTime.Now}, Binary File Import, {fileExtension} filetype not allowed ({file.Name}).\r\n";
                    continue;
                }

                var nameWithoutExtension = file.Name.ReplaceLastOccurrence( fileExtension, string.Empty );
                var parsedFileName = nameWithoutExtension.Split( '_' );
                // Person Document filenames should follow this pattern:
                // 0. PersonForeignId
                // 1. DocumentType
                // 2. DocumentForeignId
                // 3. Filename
                // 4. DocumentName
                // 5. DateCreated (YYYYMMDD format): Optional

                if ( parsedFileName.Length < 5 )
                {
                    errors += $"{DateTime.Now}, Binary File Import, Filename '{file.Name}' is not in expected format.\r\n";
                    continue;
                }

                if ( parsedFileName.Length > 6 )
                {
                    errors += $"{DateTime.Now}, Binary File Import, Filename '{file.Name}' is not in expected format. It has more segments than expected. Result of importing this file may be unexpected.\r\n";
                    continue;
                }

                var personForeignId = parsedFileName[0];
                var personKeys = ImportedPeople.FirstOrDefault( p => p.PersonForeignKey == string.Format( "{0}^{1}", importInstanceFKPrefix, personForeignId ) );
                if ( personKeys == null )
                {
                    errors += $"{DateTime.Now}, Binary File Import, Foreign Person Id '{personForeignId}' not found in Rock. File '{file.Name}' was not imported.\r\n";
                    continue;
                }
                else
                {
                    var documentTypeString = parsedFileName[1];
                    var docTypeTrimmed = documentTypeString.Replace( " ", string.Empty );
                    DocumentType documentType = existingDocTypeDict.GetValueOrNull( docTypeTrimmed );
                    if ( documentType == null )
                    {
                        errors += $"{DateTime.Now}, Binary File Import, DocuemntType '{documentTypeString}' was not found. Filename '{file.Name}' was not imported.\r\n";
                        continue;
                    }

                    var documentForeignId = parsedFileName[2];
                    var filename = $"{parsedFileName[3]}{fileExtension}";
                    var documentName = parsedFileName[4];
                    var foreignKey = $"{ImportInstanceFKPrefix}^{documentForeignId}_{docTypeTrimmed}".Left( 100 );

                    if ( existingBinaryFileFKs.Any( fk => fk == foreignKey ) )
                    {
                        errors += $"{DateTime.Now}, Binary File Import, Binary file with ForeignKey '{foreignKey}' already exists. Filename '{file.Name}' was not imported.\r\n";
                        continue;
                    }

                    var documentCreatedDate = RockDateTime.Now;
                    if ( parsedFileName.Count() > 5 )
                    {
                        var invalidDate = false;
                        var dateStringChars = parsedFileName[5].ReplaceLastOccurrence( fileExtension, string.Empty ).ToCharArray();
                        if ( dateStringChars.Count() == 8 )
                        {
                            var dateString = $"{dateStringChars[4]}{dateStringChars[5]}/{dateStringChars[6]}{dateStringChars[7]}/{dateStringChars[0]}{dateStringChars[1]}{dateStringChars[2]}{dateStringChars[3]}";
                            var createdDate = dateString.AsDateTime();
                            if ( createdDate.HasValue )
                            {
                                documentCreatedDate = createdDate.Value;
                            }
                            else
                            {
                                invalidDate = true;
                            }
                        }
                        else
                        {
                            invalidDate = true;
                        }
                        if ( invalidDate )
                        {
                            errors += $"{DateTime.Now}, Binary File Import, Invalid date string ({parsedFileName[5]}) detected in filename '{file.Name}'. File was imported with CreatedDate defaulted to import date.\r\n";
                        }
                    }

                    var rockFile = new Rock.Model.BinaryFile
                    {
                        MimeType = GetMIMEType( file.Name ),
                        BinaryFileTypeId = personDocBinaryFileType.Id,
                        FileName = filename,
                        Description = string.Format( "Imported as {0}", filename ),
                        CreatedDateTime = documentCreatedDate,
                        ModifiedDateTime = documentCreatedDate,
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        Guid = Guid.NewGuid(),
                        ForeignKey = foreignKey
                    };

                    rockFile.SetStorageEntityTypeId( personDocBinaryFileType.StorageEntityTypeId );

                    if ( personDocBinaryFileType.AttributeValues != null )
                    {
                        rockFile.StorageEntitySettings = personDocBinaryFileType.AttributeValues
                            .ToDictionary( a => a.Key, v => v.Value.Value ).ToJson();
                    }

                    if ( rockFile.StorageProvider == null )
                    {
                        errors += $"{DateTime.Now}, Binary File Import, Could not load storage provider for filename '{file.Name}'. Document was not imported.\r\n";
                        continue;
                    }

                    rockFile.StorageProvider.SaveContent( rockFile );
                    rockFile.Path = rockFile.StorageProvider.GetPath( rockFile );
                    newBinaryFiles.Add( rockFile );

                    var newDocumentKeys = new DocumentKeys()
                    {
                        PersonId = personKeys.PersonId,
                        DocumentTypeId = documentType.Id,
                        DocumentForeignKey = foreignKey,
                        DocumentName = documentName,
                        DocumentDate = documentCreatedDate,
                        File = rockFile
                    };

                    // use base stream instead of file stream to keep the byte[]
                    using ( var fileContent = new StreamReader( file.Open() ) )
                    {
                        newDocumentKeys.DocumentData = Convert.ToBase64String( fileContent.BaseStream.ReadBytesToEnd() );
                    }

                    newFileList.Add( newDocumentKeys );

                    existingBinaryFileFKs.Add( foreignKey );
                }
            }

            rockContext.BulkInsert( newBinaryFiles );

            existingBinaryFileDict = LoadBinaryFileDict( rockContext, importInstanceFKPrefix, out existingBinaryFileFKs );

            var newBinaryFileDatas = new List<Rock.Model.BinaryFileData>();
            var newDocuments = new List<Document>();
            foreach ( var entry in newFileList.Where( f => f.File != null && f.File.BinaryFileTypeId != null ) )
            {
                var binaryFile = existingBinaryFileDict.GetValueOrNull( entry.File.Guid );

                // Null file should not happen, but handling and reporting it just in case.
                if ( binaryFile == null )
                {
                    errors += $"{DateTime.Now}, Binary File Import, No binary file found for document '{entry.DocumentForeignKey}'. No new document was created for Filename '{entry.File.FileName}'.\r\n";
                    continue;
                }

                var newBinaryFileData = new BinaryFileData()
                {
                    Id = binaryFile.Id,
                    Content = Convert.FromBase64String( entry.DocumentData ),
                    CreatedDateTime = entry.DocumentDate,
                    ModifiedDateTime = entry.DocumentDate,
                    ForeignKey = entry.DocumentForeignKey
                };

                newBinaryFileDatas.Add( newBinaryFileData );

                var document = existingDocumentList.FirstOrDefault( d => d.ForeignKey == entry.DocumentForeignKey );
                if ( document != null )
                {
                    errors += $"{DateTime.Now}, Binary File Import, Document with ForeignKey '{entry.DocumentForeignKey}' already exists. No new document was created for Filename '{entry.File.FileName}'.\r\n";
                    continue;
                }

                document = new Document
                {
                    Name = entry.DocumentName,
                    EntityId = entry.PersonId,
                    DocumentTypeId = entry.DocumentTypeId.Value,
                    CreatedDateTime = entry.DocumentDate.Value,
                    BinaryFile = binaryFile,
                    ForeignKey = entry.File.ForeignKey,
                    ForeignId = entry.File.ForeignId,
                    IsSystem = false
                };

                var isValid = document.IsValid;
                if ( !isValid )
                {
                    errors += $"{DateTime.Now}, Binary File Import, An error was encountered when trying to create the document for filename {entry.File.FileName}': {document.ValidationResults.Select( a => a.ErrorMessage ).ToList().AsDelimited( "\r\n" )}\r\n";
                    continue;
                }

                newDocuments.Add( document );
            }

            rockContext.BulkInsert( newBinaryFileDatas );

            if ( newDocuments.Any() )
            {
                rockContext.Documents.AddRange( newDocuments );
                rockContext.SaveChanges();

                existingDocumentList = LoadDocumentList( rockContext, importInstanceFKPrefix );

                // Set security on binary files to use related document

                var documentEntityTypeId = EntityTypeCache.GetId( Rock.SystemGuid.EntityType.DOCUMENT );
                var documentInfo = existingDocumentList.ToDictionary( d => d.Guid, d => d.Id );
                foreach ( var document in newDocuments )
                {
                    var binaryFile = existingBinaryFileDict.GetValueOrNull( document.BinaryFile.Guid );
                    binaryFile.ParentEntityTypeId = documentEntityTypeId;
                    binaryFile.ParentEntityId = documentInfo.GetValueOrNull( document.Guid );
                }

                rockContext.SaveChanges();
            }
            return importFiles.Count;
        }
    }
}