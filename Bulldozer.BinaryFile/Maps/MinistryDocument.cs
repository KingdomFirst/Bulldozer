// <copyright>
// Copyright 2019 by Kingdom First Solutions
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
using System.Text.RegularExpressions;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Storage;
using Rock.Web.Cache;
using static Bulldozer.Utility.Extensions;
using Attribute = Rock.Model.Attribute;

namespace Bulldozer.BinaryFile
{
    /// <summary>
    /// Maps Ministry Documents
    /// </summary>
    public class MinistryDocument : BinaryFileComponent, IBinaryFile
    {
        /// <summary>
        /// Maps the specified folder.
        /// </summary>
        /// <param name="folder">The folder.</param>
        /// <param name="ministryFileType">Type of the ministry file.</param>
        public int Map( ZipArchive folder, BinaryFileType ministryFileType )
        {
            var lookupContext = new RockContext();
            var personEntityTypeId = EntityTypeCache.GetId<Person>();
            var binaryFileTypeService = new BinaryFileTypeService( lookupContext );
            var fileFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.FILE.AsGuid(), lookupContext ).Id;
            var backgroundFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.BACKGROUNDCHECK.AsGuid(), lookupContext ).Id;

            var existingAttributes = new AttributeService( lookupContext ).GetByFieldTypeId( fileFieldTypeId )
                .Where( a => a.EntityTypeId == personEntityTypeId )
                .ToDictionary( a => a.Key, a => a );

            var backgroundCheckFileAttributes = new AttributeService( lookupContext ).GetByFieldTypeId( backgroundFieldTypeId )
                .Where( a => a.EntityTypeId == personEntityTypeId )
                .ToDictionary( a => a.Key, a => a );

            foreach ( var backgroundCheckFileAttribute in backgroundCheckFileAttributes )
            {
                if ( !existingAttributes.ContainsKey( backgroundCheckFileAttribute.Key ) )
                {
                    existingAttributes.Add( backgroundCheckFileAttribute.Key, backgroundCheckFileAttribute.Value );
                }
            }

            var emptyJsonObject = "{}";
            var newFileList = new List<DocumentKeys>();

            var completedItems = 0;
            var totalRows = folder.Entries.Count;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying ministry document import ({0:N0} found)", totalRows ) );

            foreach ( var file in folder.Entries.OrderBy( f => f.Name ) )
            {
                var fileExtension = Path.GetExtension( file.Name );
                if ( FileTypeBlackList.Contains( fileExtension ) )
                {
                    LogException( "Binary File Import", string.Format( "{0} filetype not allowed ({1})", fileExtension, file.Name ) );
                    continue;
                }

                var nameWithoutExtension = file.Name.ReplaceLastOccurrence( fileExtension, string.Empty );
                var parsedFileName = nameWithoutExtension.Split( '_' );
                // Ministry docs should follow this pattern:
                // 0. Firstname
                // 1. Lastname
                // 2. ForeignId
                // 3. Filename
                // 4. Doc Id
                if ( parsedFileName.Length < 3 )
                {
                    break;
                }

                var personForeignId = parsedFileName[2].AsType<int?>();
                var personKeys = ImportedPeople.FirstOrDefault( p => p.PersonForeignId == personForeignId );
                if ( personKeys != null )
                {
                    var attributeName = string.Empty;
                    var documentForeignId = string.Empty;
                    if ( parsedFileName.Count() > 4 )
                    {
                        attributeName = parsedFileName[3];
                        documentForeignId = parsedFileName[4];
                    }
                    else
                    {
                        var filename = parsedFileName[3].ReplaceLastOccurrence( fileExtension, string.Empty );
                        attributeName = Regex.Replace( filename, "\\d{4,}[.\\w]+$", string.Empty );
                        documentForeignId = Regex.Match( filename, "\\d+$" ).Value;
                    }

                    // append "Document" to attribute name to create unique attributes
                    // this matches core attribute "Background Check Document"
                    attributeName = !attributeName.EndsWith( "Document", StringComparison.CurrentCultureIgnoreCase ) ? string.Format( "{0} Document", attributeName ) : attributeName;
                    var attributeKey = attributeName.RemoveSpecialCharacters();

                    Attribute fileAttribute = null;
                    var attributeBinaryFileType = ministryFileType;
                    if ( !existingAttributes.ContainsKey( attributeKey ) )
                    {
                        fileAttribute = new Attribute
                        {
                            FieldTypeId = fileFieldTypeId,
                            EntityTypeId = personEntityTypeId,
                            EntityTypeQualifierColumn = string.Empty,
                            EntityTypeQualifierValue = string.Empty,
                            Key = attributeKey,
                            Name = attributeName,
                            Description = string.Format( "{0} created by binary file import", attributeName ),
                            IsGridColumn = false,
                            IsMultiValue = false,
                            IsRequired = false,
                            AllowSearch = false,
                            IsSystem = false,
                            Order = 0
                        };

                        fileAttribute.AttributeQualifiers.Add( new AttributeQualifier()
                        {
                            Key = "binaryFileType",
                            Value = ministryFileType.Guid.ToString()
                        } );

                        lookupContext.Attributes.Add( fileAttribute );
                        lookupContext.SaveChanges();

                        existingAttributes.Add( fileAttribute.Key, fileAttribute );
                    }
                    else
                    {
                        // if attribute already exists in Rock, override default file type with the Rock-specified file type
                        fileAttribute = existingAttributes[attributeKey];
                        var attributeBinaryFileTypeGuid = fileAttribute.AttributeQualifiers.FirstOrDefault( q => q.Key.Equals( "binaryFileType" ) );
                        if ( attributeBinaryFileTypeGuid != null )
                        {
                            attributeBinaryFileType = binaryFileTypeService.Get( attributeBinaryFileTypeGuid.Value.AsGuid() );
                        }
                    }

                    var rockFile = new Rock.Model.BinaryFile
                    {
                        IsSystem = false,
                        IsTemporary = false,
                        MimeType = GetMIMEType( file.Name ),
                        BinaryFileTypeId = attributeBinaryFileType.Id,
                        FileName = file.Name,
                        Description = string.Format( "Imported as {0}", file.Name ),
                        CreatedDateTime = file.LastWriteTime.DateTime,
                        ModifiedDateTime = file.LastWriteTime.DateTime,
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ForeignKey = documentForeignId,
                        ForeignId = documentForeignId.AsIntegerOrNull()
                    };

                    rockFile.SetStorageEntityTypeId( attributeBinaryFileType.StorageEntityTypeId );
                    rockFile.StorageEntitySettings = emptyJsonObject;

                    if ( attributeBinaryFileType.AttributeValues != null )
                    {
                        rockFile.StorageEntitySettings = attributeBinaryFileType.AttributeValues
                            .ToDictionary( a => a.Key, v => v.Value.Value ).ToJson();
                    }

                    // use base stream instead of file stream to keep the byte[]
                    // NOTE: if byte[] converts to a string it will corrupt the stream
                    using ( var fileContent = new StreamReader( file.Open() ) )
                    {
                        rockFile.ContentStream = new MemoryStream( fileContent.BaseStream.ReadBytesToEnd() );
                    }

                    newFileList.Add( new DocumentKeys()
                    {
                        PersonId = personKeys.PersonId,
                        AttributeId = fileAttribute.Id,
                        File = rockFile
                    } );

                    completedItems++;

                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} ministry document files imported ({1}% complete).", completedItems, percentComplete ) );
                    }

                    if ( completedItems % ReportingNumber < 1 )
                    {
                        SaveFiles( newFileList );

                        // Reset list
                        newFileList.Clear();
                        ReportPartialProgress();
                    }
                }
            }

            if ( newFileList.Any() )
            {
                SaveFiles( newFileList );
            }

            ReportProgress( 100, string.Format( "Finished documents import: {0:N0} ministry documents imported.", completedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the files.
        /// </summary>
        /// <param name="newFileList">The new file list.</param>
        private static void SaveFiles( List<DocumentKeys> newFileList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.BinaryFiles.AddRange( newFileList.Select( f => f.File ) );
                rockContext.SaveChanges( DisableAuditing );

                foreach ( var entry in newFileList.Where( f => f.File != null && f.File.BinaryFileTypeId != null ) )
                {
                    // if a prior document exists with a more recent timestamp or document id, don't overwrite
                    var attributeValue = rockContext.AttributeValues.FirstOrDefault( p => p.AttributeId == entry.AttributeId && p.EntityId == entry.PersonId );
                    attributeValue = attributeValue ?? rockContext.AttributeValues.Local.FirstOrDefault( p => p.AttributeId == entry.AttributeId && p.EntityId == entry.PersonId );
                    if ( attributeValue == null || attributeValue.CreatedDateTime < entry.File.CreatedDateTime || attributeValue.ForeignId < entry.File.ForeignId )
                    {
                        var storageProvider = entry.File.StorageEntityTypeId == DatabaseProvider.EntityType.Id
                            ? ( ProviderComponent ) DatabaseProvider
                            : ( ProviderComponent ) FileSystemProvider;

                        if ( storageProvider != null )
                        {
                            storageProvider.SaveContent( entry.File );
                            entry.File.Path = storageProvider.GetPath( entry.File );

                            if ( attributeValue == null )
                            {   // create new
                                attributeValue = new AttributeValue
                                {
                                    EntityId = entry.PersonId,
                                    AttributeId = entry.AttributeId,
                                    Value = entry.File.Guid.ToString(),
                                    CreatedDateTime = entry.File.CreatedDateTime,
                                    ForeignKey = entry.File.ForeignKey,
                                    ForeignId = entry.File.ForeignId,
                                    IsSystem = false
                                };

                                rockContext.AttributeValues.Add( attributeValue );
                            }
                            else
                            {   // update existing
                                attributeValue.Value = entry.File.Guid.ToString();
                                attributeValue.CreatedDateTime = entry.File.CreatedDateTime;
                                attributeValue.ForeignKey = entry.File.ForeignKey;
                                attributeValue.ForeignId = entry.File.ForeignId;
                                attributeValue.IsSystem = false;
                            }
                        }
                        else
                        {
                            LogException( "Binary File Import", string.Format( "Could not load provider {0}.", storageProvider.ToString() ) );
                        }
                    }
                }

                rockContext.SaveChanges( DisableAuditing );
            } );
        }
    }
}