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
using Rock.Web.Cache;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.BinaryFile.GroupImage
{
    /// <summary>
    /// Maps Person Images
    /// </summary>
    public class GroupImage : BinaryFileComponent, IBinaryFile
    {
        /// <summary>
        /// Maps the specified folder.
        /// </summary>
        /// <param name="folder">The folder.</param>
        /// <param name="groupImageType">Type of the group image file.</param>
        public int Map( ZipArchive folder, BinaryFileType groupImageType, int chunkSize, string importInstanceFKPrefix )
        {
            // check for existing images
            var lookupContext = new RockContext();
            var groupEntityTypeId = EntityTypeCache.GetId<Rock.Model.Group>();
            var imageFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.IMAGE.AsGuid(), lookupContext ).Id;
            var binaryFileTypeService = new BinaryFileTypeService( lookupContext );

            var existingGroupImageAttributes = new AttributeService( lookupContext ).GetByFieldTypeId( imageFieldTypeId )
                .Where( a => a.EntityTypeId == groupEntityTypeId && a.FieldTypeId == groupImageType.Id && a.Name == "Group Image" )
                .ToDictionary( a => a.Key, a => a );

            var emptyJsonObject = "{}";
            var newFileList = new List<GroupDocumentKeys>();
            var importedGroups = new GroupService( lookupContext )
                .Queryable().AsNoTracking().Where( g => g.ForeignKey != null && g.ForeignKey.StartsWith( importInstanceFKPrefix + "^G_" ) );

            var completedItems = 0;
            var totalEntries = folder.Entries.Count;
            var percentage = ( totalEntries - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying group images import ({0:N0} found.)", totalEntries ) );

            foreach ( var file in folder.Entries )
            {
                var fileExtension = Path.GetExtension( file.Name );
                if ( FileTypeBlackList.Contains( fileExtension ) )
                {
                    LogException( "Binary File Import", string.Format( "{0} filetype not allowed ({1})", fileExtension, file.Name ) );
                    continue;
                }
                var fileName = Path.GetFileNameWithoutExtension( file.Name );
                var groupForeignId = new Regex( @"\D" ).Replace( fileName ?? "", "" ).AsIntegerOrNull();
                var group = importedGroups.FirstOrDefault( g => g.ForeignKey == importInstanceFKPrefix + "^G_" + groupForeignId );
                if ( group != null )
                {
                    var attributeName = "Group Image";
                    var attributeKey = $"{attributeName.RemoveSpecialCharacters()}_{group.GroupTypeId}";
                    var attributeForeignKey = $"{importInstanceFKPrefix}^{attributeKey}".Left( 100 );

                    Attribute groupImageAttribute = null;
                    var attributeBinaryFileType = groupImageType;

                    // Check for the Group Attribute and add if it is missing.
                    if ( !existingGroupImageAttributes.ContainsKey( attributeKey ) )
                    {
                        groupImageAttribute = new Attribute
                        {
                            FieldTypeId = imageFieldTypeId,
                            EntityTypeId = groupEntityTypeId,
                            EntityTypeQualifierColumn = "GroupTypeId",
                            EntityTypeQualifierValue = group.GroupTypeId.ToString(),
                            Key = attributeKey,
                            Name = attributeName,
                            Description = string.Format( "{0} created by binary file import", attributeName ),
                            IsGridColumn = false,
                            IsMultiValue = false,
                            IsRequired = false,
                            AllowSearch = false,
                            IsSystem = false,
                            Order = 0,
                            ForeignKey = attributeForeignKey
                        };

                        groupImageAttribute.AttributeQualifiers.Add( new AttributeQualifier()
                        {
                            Key = "binaryFileType",
                            Value = groupImageType.Guid.ToString()
                        } );

                        lookupContext.Attributes.Add( groupImageAttribute );
                        lookupContext.SaveChanges();

                        existingGroupImageAttributes.Add( groupImageAttribute.Key, groupImageAttribute );
                    }
                    else
                    {
                        // if attribute already exists in Rock, override default file type with the Rock-specified file type
                        groupImageAttribute = existingGroupImageAttributes[attributeKey];
                        var attributeBinaryFileTypeGuid = groupImageAttribute.AttributeQualifiers.FirstOrDefault( q => q.Key.Equals( "binaryFileType" ) );
                        if ( attributeBinaryFileTypeGuid != null )
                        {
                            attributeBinaryFileType = binaryFileTypeService.Get( attributeBinaryFileTypeGuid.Value.AsGuid() );
                        }
                    }

                    var rockFile = new Rock.Model.BinaryFile()
                    {
                        IsSystem = false,
                        IsTemporary = false,
                        MimeType = GetMIMEType( file.Name ),
                        BinaryFileTypeId = groupImageType.Id,
                        FileName = file.Name,
                        Description = string.Format( "Imported as {0}", file.Name ),
                        CreatedDateTime = file.LastWriteTime.DateTime,
                        ModifiedDateTime = file.LastWriteTime.DateTime,
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ForeignKey = $"{importInstanceFKPrefix}^{attributeName.RemoveSpecialCharacters()}_{group.Id}".Left( 100 )
                    };

                    rockFile.SetStorageEntityTypeId( groupImageType.StorageEntityTypeId );
                    rockFile.StorageEntitySettings = emptyJsonObject;

                    if ( groupImageType.AttributeValues.Any() )
                    {
                        rockFile.StorageEntitySettings = groupImageType.AttributeValues
                            .ToDictionary( a => a.Key, v => v.Value.Value ).ToJson();
                    }

                    // use base stream instead of file stream to keep the byte[]
                    // NOTE: if byte[] converts to a string it will corrupt the stream
                    using ( var fileContent = new StreamReader( file.Open() ) )
                    {
                        rockFile.ContentStream = new MemoryStream( fileContent.BaseStream.ReadBytesToEnd() );
                    }

                    newFileList.Add( new GroupDocumentKeys()
                    {
                        GroupId = group.Id,
                        AttributeId = groupImageAttribute.Id,
                        File = rockFile
                    } );

                    completedItems++;
                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} group image files imported ({1}% complete).", completedItems, percentComplete ) );
                    }

                    if ( completedItems % chunkSize < 1 )
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

            ReportProgress( 100, string.Format( "Finished group images import: {0:N0} group images imported.", completedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the files.
        /// </summary>
        /// <param name="newFileList">The new file list.</param>
        private static void SaveFiles( List<GroupDocumentKeys> newFileList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.BinaryFiles.AddRange( newFileList.Where( f => f.File != null && f.File.BinaryFileTypeId != null ).Select( f => f.File ) );
                rockContext.SaveChanges( DisableAuditing );
                foreach ( var entry in newFileList.Where( f => f.File != null && f.File.BinaryFileTypeId != null ) )
                {
                    // if a prior document exists with a more recent timestamp or document id, don't overwrite
                    var attributeValue = rockContext.AttributeValues.FirstOrDefault( p => p.AttributeId == entry.AttributeId && p.EntityId == entry.GroupId );
                    attributeValue = attributeValue ?? rockContext.AttributeValues.Local.FirstOrDefault( p => p.AttributeId == entry.AttributeId && p.EntityId == entry.GroupId );
                    if ( attributeValue == null || attributeValue.CreatedDateTime < entry.File.CreatedDateTime )
                    {

                        ProviderComponent storageProvider;
                        if ( entry.File.StorageEntityTypeId == DatabaseProvider.EntityType.Id )
                        {
                            storageProvider = ( ProviderComponent ) DatabaseProvider;
                        }
                        else if ( entry.File.StorageEntityTypeId == AzureBlobStorageProvider.EntityType.Id )
                        {
                            storageProvider = ( ProviderComponent ) AzureBlobStorageProvider;
                        }
                        else
                        {
                            storageProvider = ( ProviderComponent ) FileSystemProvider;
                        }

                        if ( storageProvider != null )
                        {
                            storageProvider.SaveContent( entry.File );
                            entry.File.Path = storageProvider.GetPath( entry.File );

                            if ( attributeValue == null )
                            {   // create new
                                attributeValue = new AttributeValue
                                {
                                    EntityId = entry.GroupId,
                                    AttributeId = entry.AttributeId,
                                    Value = entry.File.Guid.ToString(),
                                    CreatedDateTime = entry.File.CreatedDateTime,
                                    IsSystem = false,
                                    ForeignKey = entry.File.ForeignKey
                                };

                                rockContext.AttributeValues.Add( attributeValue );
                            }
                            else
                            {   // update existing
                                attributeValue.Value = entry.File.Guid.ToString();
                                attributeValue.CreatedDateTime = entry.File.CreatedDateTime;
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