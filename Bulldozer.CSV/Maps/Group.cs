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
using System.Data.Entity;
using System.Data.Entity.Spatial;
using System.Globalization;
using System.Linq;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the group import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Loads the group data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadGroup( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var locationService = new LocationService( lookupContext );
            var groupTypeService = new GroupTypeService( lookupContext );

            var topicTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.SMALL_GROUP_TOPIC ), lookupContext ).DefinedValues;

            var numImportedGroups = ImportedGroups.Count();

            var newGroupLocations = new Dictionary<GroupLocation, string>();
            var currentGroup = new Group();

            // Look for custom attributes in the Individual file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > GroupCapacity )
                .ToDictionary( f => f.index, f => f.node.Name );

            var groupAttributes = new AttributeService( lookupContext ).GetByEntityTypeId( new Group().TypeId ).ToList();
            var completed = 0;

            ReportProgress( 0, $"Starting group import ({numImportedGroups:N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowGroupKey = row[GroupId];

                //
                // Determine if we are still working with the same group or not.
                //
                if ( rowGroupKey != null && rowGroupKey != currentGroup.ForeignKey )
                {
                    currentGroup = LoadGroupBasic( lookupContext, rowGroupKey, row[GroupName], row[GroupCreatedDate], row[GroupType], row[GroupParentGroupId], row[GroupActive], row[GroupDescription] );

                    //
                    // Set the group campus
                    //
                    var campusName = row[GroupCampus];
                    if ( !string.IsNullOrWhiteSpace( campusName ) )
                    {
                        var groupCampus = CampusList.FirstOrDefault( c => c.Name.Equals( campusName, StringComparison.OrdinalIgnoreCase )
                            || c.ShortCode.Equals( campusName, StringComparison.OrdinalIgnoreCase ) );
                        if ( groupCampus == null )
                        {
                            groupCampus = new Campus
                            {
                                IsSystem = false,
                                Name = campusName,
                                ShortCode = campusName.RemoveWhitespace(),
                                IsActive = true
                            };
                            lookupContext.Campuses.Add( groupCampus );
                            lookupContext.SaveChanges( DisableAuditing );
                            CampusList.Add( groupCampus );
                        }

                        currentGroup.CampusId = groupCampus.Id;
                    }

                    //
                    // If the group type has one or more location types defined then import the
                    // primary address as the first location type.
                    //
                    var existingLocationIds = new GroupLocationService( lookupContext ).Queryable().AsNoTracking().Where( gl => gl.GroupId == currentGroup.Id ).Select( gl => gl.LocationId ).ToList();
                    var groupType = groupTypeService.Get( currentGroup.GroupTypeId );
                    if ( groupType.LocationTypes.Count > 0 && ( !string.IsNullOrWhiteSpace( row[GroupAddress] ) || !string.IsNullOrWhiteSpace( row[GroupNamedLocation] ) ) && currentGroup.GroupLocations.Count == 0 )
                    {
                        var primaryLocationTypeId = groupType.LocationTypes.ToList()[0].LocationTypeValueId;

                        var grpAddress = row[GroupAddress];
                        var grpAddress2 = row[GroupAddress2];
                        var grpCity = row[GroupCity];
                        var grpState = row[GroupState];
                        var grpZip = row[GroupZip];
                        var grpCountry = row[GroupCountry];

                        var namedLocation = row[GroupNamedLocation];

                        if ( string.IsNullOrWhiteSpace( namedLocation ) )
                        {
                            Location primaryAddress = GetOrAddLocation( lookupContext, grpAddress, grpAddress2, grpCity, grpState, grpZip, grpCountry );

                            if ( primaryAddress != null && !existingLocationIds.Contains( primaryAddress.Id ) )
                            {
                                var primaryLocation = new GroupLocation
                                {
                                    LocationId = primaryAddress.Id,
                                    IsMailingLocation = true,
                                    IsMappedLocation = true,
                                    GroupLocationTypeValueId = primaryLocationTypeId
                                };
                                newGroupLocations.Add( primaryLocation, rowGroupKey );
                            }
                            else if ( !string.IsNullOrWhiteSpace( grpAddress ) || !string.IsNullOrWhiteSpace( grpCity ) || !string.IsNullOrWhiteSpace( grpState ) )
                            {
                                var missingAddrParts = new List<string>();
                                if ( string.IsNullOrWhiteSpace( grpAddress ) )
                                {
                                    missingAddrParts.Add( "Address" );
                                }
                                if ( string.IsNullOrWhiteSpace( grpCity ) )
                                {
                                    missingAddrParts.Add( "City" );
                                }
                                if ( string.IsNullOrWhiteSpace( grpState ) )
                                {
                                    missingAddrParts.Add( "State/Province" );
                                }
                                LogException( "Group Import", string.Format( "Invalid primary address for group \"{0}\". Missing {1}. Address not imported.", rowGroupKey, string.Join( ", ", missingAddrParts ) ) );
                            }
                        }
                        else
                        {
                            var primaryAddress = locationService.Queryable().AsNoTracking().FirstOrDefault( l => l.Name.Equals( namedLocation ) || l.ForeignKey.Equals( namedLocation ) );
                            if ( primaryAddress != null && !existingLocationIds.Contains( primaryAddress.Id ) )
                            {
                                var primaryLocation = new GroupLocation
                                {
                                    LocationId = primaryAddress.Id,
                                    IsMailingLocation = true,
                                    IsMappedLocation = true,
                                    GroupLocationTypeValueId = primaryLocationTypeId
                                };
                                newGroupLocations.Add( primaryLocation, rowGroupKey );
                            }
                            else
                            {
                                LogException( "Group Import", string.Format( "The named location {0} was not found and will not be mapped.", namedLocation ) );
                            }
                        }
                    }

                    //
                    // If the group type has two or more location types defined then import the
                    // secondary address as the group type's second location type.
                    //
                    if ( groupType.LocationTypes.Count > 1 && !string.IsNullOrWhiteSpace( row[GroupSecondaryAddress] ) && currentGroup.GroupLocations.Count < 2 )
                    {
                        var secondaryLocationTypeId = groupType.LocationTypes.ToList()[1].LocationTypeValueId;

                        var grpSecondAddress = row[GroupSecondaryAddress];
                        var grpSecondAddress2 = row[GroupSecondaryAddress2];
                        var grpSecondCity = row[GroupSecondaryCity];
                        var grpSecondState = row[GroupSecondaryState];
                        var grpSecondZip = row[GroupSecondaryZip];
                        var grpSecondCountry = row[GroupSecondaryCountry];

                        Location secondaryAddress = GetOrAddLocation( lookupContext, grpSecondAddress, grpSecondAddress2, grpSecondCity, grpSecondState, grpSecondZip, grpSecondCountry );

                        if ( secondaryAddress != null && !existingLocationIds.Contains( secondaryAddress.Id ) )
                        {
                            var secondaryLocation = new GroupLocation
                            {
                                LocationId = secondaryAddress.Id,
                                IsMailingLocation = true,
                                IsMappedLocation = true,
                                GroupLocationTypeValueId = secondaryLocationTypeId
                            };
                            newGroupLocations.Add( secondaryLocation, rowGroupKey );
                        }
                        else if ( !string.IsNullOrWhiteSpace( grpSecondAddress ) || !string.IsNullOrWhiteSpace( grpSecondCity ) || !string.IsNullOrWhiteSpace( grpSecondState ) )
                        {
                            var missingAddrParts = new List<string>();
                            if ( string.IsNullOrWhiteSpace( grpSecondAddress ) )
                            {
                                missingAddrParts.Add( "Address" );
                            }
                            if ( string.IsNullOrWhiteSpace( grpSecondCity ) )
                            {
                                missingAddrParts.Add( "City" );
                            }
                            if ( string.IsNullOrWhiteSpace( grpSecondState ) )
                            {
                                missingAddrParts.Add( "State/Province" );
                            }
                            LogException( "Group Import", string.Format( "Invalid secondary address for group \"{0}\". Missing {1}. Address not imported.", rowGroupKey, string.Join( ", ", missingAddrParts ) ) );
                        }
                    }

                    //
                    // Set the group's sorting order.
                    //
                    var groupOrder = 9999;
                    int.TryParse( row[GroupOrder], out groupOrder );
                    currentGroup.Order = groupOrder;

                    //
                    // Set the group's capacity
                    //
                    var capacity = row[GroupCapacity].AsIntegerOrNull();
                    if ( capacity.HasValue )
                    {
                        currentGroup.GroupCapacity = capacity;

                        if ( groupType.GroupCapacityRule == GroupCapacityRule.None )
                        {
                            groupType.GroupCapacityRule = GroupCapacityRule.Hard;
                        }
                    }

                    //
                    // Set the group's schedule
                    //
                    if ( !string.IsNullOrWhiteSpace( row[GroupDayOfWeek] ) )
                    {
                        DayOfWeek dayEnum;
                        if ( Enum.TryParse( row[GroupDayOfWeek], true, out dayEnum ) )
                        {
                            if ( groupType.AllowedScheduleTypes != ScheduleType.Weekly )
                            {
                                groupType.AllowedScheduleTypes = ScheduleType.Weekly;
                            }
                            var day = dayEnum;
                            var time = row[GroupTime].AsDateTime();
                            currentGroup.ScheduleId = AddNamedSchedule( lookupContext, string.Empty, string.Empty, day, time, null, rowGroupKey ).Id;
                        }
                    }

                    //
                    // Assign Attributes
                    //
                    if ( customAttributes.Any() )
                    {
                        lookupContext.SaveChanges();

                        foreach ( var attributePair in customAttributes )
                        {
                            var pairs = attributePair.Value.Split( '^' );
                            var categoryName = string.Empty;
                            var attributeName = string.Empty;
                            var attributeTypeString = string.Empty;
                            var attributeForeignKey = string.Empty;
                            var definedValueForeignKey = string.Empty;
                            var fieldTypeId = TextFieldTypeId;

                            if ( pairs.Length == 1 )
                            {
                                attributeName = pairs[0];
                            }
                            else if ( pairs.Length == 2 )
                            {
                                attributeName = pairs[0];
                                attributeTypeString = pairs[1];
                            }
                            else if ( pairs.Length >= 3 )
                            {
                                categoryName = pairs[1];
                                attributeName = pairs[2];
                                if ( pairs.Length >= 4 )
                                {
                                    attributeTypeString = pairs[3];
                                }
                                if ( pairs.Length >= 5 )
                                {
                                    attributeForeignKey = pairs[4];
                                }
                                if ( pairs.Length >= 6 )
                                {
                                    definedValueForeignKey = pairs[5];
                                }
                            }

                            var definedValueForeignId = definedValueForeignKey.AsType<int?>();

                            //
                            // Translate the provided attribute type into one we know about.
                            //
                            fieldTypeId = GetAttributeFieldType( attributeTypeString );

                            Rock.Model.Attribute currentAttribute = null;
                            if ( string.IsNullOrEmpty( attributeName ) )
                            {
                                LogException( "Group Attribute", string.Format( "Group Attribute Name cannot be blank '{0}'.", attributePair.Value ) );
                            }
                            else
                            {
                                if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                                {
                                    attributeForeignKey = string.Format( "Bulldozer_{0}_{1}_{2}", groupType.Id, categoryName.RemoveWhitespace(), attributeName.RemoveWhitespace() ).Left( 100 );
                                }
                                currentAttribute = groupAttributes.FirstOrDefault( a =>
                                    a.Name.Equals( attributeName, StringComparison.OrdinalIgnoreCase )
                                    && a.FieldTypeId == fieldTypeId
                                    && a.EntityTypeId == currentGroup.TypeId
                                    && a.EntityTypeQualifierValue == groupType.Id.ToString()
                                );
                                if ( currentAttribute == null )
                                {
                                    currentAttribute = AddEntityAttribute( lookupContext, currentGroup.TypeId, "GroupTypeId", groupType.Id.ToString(), attributeForeignKey, categoryName, attributeName, string.Empty, fieldTypeId, true, definedValueForeignId, definedValueForeignKey, attributeTypeString: attributeTypeString );
                                    groupAttributes.Add( currentAttribute );
                                }

                                var attributeValue = row[attributePair.Key];
                                if ( !string.IsNullOrEmpty( attributeValue ) )
                                {
                                    AddEntityAttributeValue( lookupContext, currentAttribute, currentGroup, row[attributePair.Key], null, true );
                                }
                            }
                        }
                    }

                    //
                    // Changes to groups need to be saved right away since one group
                    // will reference another group.
                    //
                    lookupContext.SaveChanges();

                    //
                    // Keep the user informed as to what is going on and save in batches.
                    //
                    completed++;
                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} groups imported." );
                    }

                    if ( completed % ReportingNumber < 1 )
                    {
                        SaveGroupLocations( newGroupLocations );
                        ReportPartialProgress();

                        // Reset lookup context
                        lookupContext.SaveChanges();
                        lookupContext = new RockContext();
                        locationService = new LocationService( lookupContext );
                        groupTypeService = new GroupTypeService( lookupContext );
                        newGroupLocations.Clear();
                    }
                }
            }

            //
            // Check to see if any rows didn't get saved to the database
            //
            if ( newGroupLocations.Any() )
            {
                SaveGroupLocations( newGroupLocations );
            }

            lookupContext.SaveChanges();
            lookupContext.Dispose();

            ReportProgress( 0, $"Finished group import: {completed:N0} groups added or updated." );

            return completed;
        }

        /// <summary>
        /// Load in the basic group information passed in by the caller. Group is not saved
        /// unless the caller explecitely save the group.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        /// <param name="groupKey">The group key.</param>
        /// <param name="name">The name.</param>
        /// <param name="createdDate">The created date.</param>
        /// <param name="type">The type.</param>
        /// <param name="parentGroupKey">The parent group key.</param>
        /// <param name="active">The active.</param>
        /// <returns></returns>
        private Group LoadGroupBasic( RockContext lookupContext, string groupKey, string name, string createdDate, string type, string parentGroupKey, string active, string description = "" )
        {
            var groupTypeId = LoadGroupTypeId( lookupContext, type );
            var groupId = groupKey.AsType<int?>();
            Group group, parent;

            //
            // See if we have already imported it previously. Otherwise
            // create it as a new group.
            //
            group = ImportedGroups.FirstOrDefault( g => g.ForeignKey == groupKey );

            // Check if this was an existing group that needs foreign id added
            if ( group == null )
            {
                var parentGroupId = ImportedGroups.FirstOrDefault( g => g.ForeignKey == parentGroupKey )?.Id;
                group = new GroupService( lookupContext ).Queryable().Where( g => g.ForeignKey == null && g.GroupTypeId == groupTypeId && g.Name.Equals( name, StringComparison.OrdinalIgnoreCase ) && g.ParentGroupId == parentGroupId ).FirstOrDefault();
            }

            if ( group == null )
            {
                group = new Group
                {
                    ForeignKey = groupKey,
                    ForeignId = groupId,
                    Name = name,
                    CreatedByPersonAliasId = ImportPersonAliasId,
                    GroupTypeId = groupTypeId,
                    Description = description
                };

                lookupContext.Groups.Add( group );
                ImportedGroups.Add( group );
            }
            else
            {
                if ( string.IsNullOrWhiteSpace( group.ForeignKey ) )
                {
                    group.ForeignKey = groupKey;
                    group.ForeignId = groupId;

                    if ( !ImportedGroups.Any( g => g.ForeignKey.Equals( groupKey, StringComparison.OrdinalIgnoreCase ) ) )
                    {
                        ImportedGroups.Add( group );
                    }
                }

                lookupContext.Groups.Attach( group );
                lookupContext.Entry( group ).State = EntityState.Modified;
            }

            //
            // Find and set the parent group. If not found it becomes a root level group.
            //
            parent = ImportedGroups.FirstOrDefault( g => g.ForeignKey == parentGroupKey );
            if ( parent != null )
            {
                group.ParentGroupId = parent.Id;
            }

            //
            // Setup the date created/modified values from the data, if we have them.
            //
            group.CreatedDateTime = ParseDateOrDefault( createdDate, ImportDateTime );
            group.ModifiedDateTime = ImportDateTime;

            //
            // Set the active state of this group.
            //
            if ( active.ToUpper() == "NO" )
            {
                group.IsActive = false;
            }
            else
            {
                group.IsActive = true;
            }

            return group;
        }

        /// <summary>
        /// Saves all group locations.
        /// </summary>
        /// <param name="newGroupLocations">The new group locations.</param>
        private void SaveGroupLocations( Dictionary<GroupLocation, string> newGroupLocations )
        {
            var rockContext = new RockContext();

            //
            // Now save any new locations
            //
            if ( newGroupLocations.Any() )
            {
                //
                // Match up the new, real, group Id for each location.
                //
                foreach ( var locationPair in newGroupLocations )
                {
                    var groupId = ImportedGroups.Where( g => g.ForeignKey == locationPair.Value ).Select( g => ( int? ) g.Id ).FirstOrDefault();
                    if ( groupId != null )
                    {
                        locationPair.Key.GroupId = ( int ) groupId;
                    }
                }

                //
                // Save locations to the database
                //
                rockContext.WrapTransaction( () =>
                {
                    rockContext.Configuration.AutoDetectChangesEnabled = false;
                    rockContext.GroupLocations.AddRange( newGroupLocations.Keys );
                    rockContext.ChangeTracker.DetectChanges();
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }

        /// <summary>
        /// Loads the group type data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadGroupType( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var newGroupTypeList = new List<GroupType>();
            var purposeTypeValues = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.GROUPTYPE_PURPOSE ), lookupContext ).DefinedValues;
            var locationMeetingId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_MEETING_LOCATION ), lookupContext ).Id;

            var numImportedGroupTypes = ImportedGroupTypes.Count();
            var completed = 0;

            ReportProgress( 0, $"Starting group type import ({numImportedGroupTypes:N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowGroupTypeName = row[GroupTypeName].Left( 100 );
                var rowGroupTypeDescription = row[GroupTypeDescription];
                var rowGroupTypeKey = row[GroupTypeId];
                var groupTypeForeignId = rowGroupTypeKey.AsType<int?>();

                // Check that this group type isn't already in our data
                var groupTypeExists = false;
                if ( numImportedGroupTypes > 0 )
                {
                    groupTypeExists = ImportedGroupTypes.Any( t => t.ForeignKey == rowGroupTypeKey );
                }

                // Check if this was an existing group type that needs foreign id added
                if ( !groupTypeExists )
                {
                    var groupType = new GroupTypeService( lookupContext ).Queryable().FirstOrDefault( t => ( t.ForeignKey == null || t.ForeignKey.Trim() == "" ) && t.Name.Equals( rowGroupTypeName, StringComparison.OrdinalIgnoreCase ) );
                    if ( groupType != null )
                    {
                        groupType.ForeignKey = rowGroupTypeKey;
                        groupType.ForeignGuid = rowGroupTypeKey.AsGuidOrNull();
                        groupType.ForeignId = rowGroupTypeKey.AsIntegerOrNull();

                        lookupContext.SaveChanges();
                        groupTypeExists = true;
                        ImportedGroupTypes.Add( groupType );
                        completed++;
                    }
                }

                if ( !groupTypeExists )
                {
                    var newGroupType = new GroupType
                    {
                        // set required properties (terms set by default)
                        IsSystem = false,
                        Name = rowGroupTypeName,
                        Description = rowGroupTypeDescription,
                        Order = 1000 + completed,

                        // set optional properties
                        CreatedDateTime = ParseDateOrDefault( row[GroupTypeCreatedDate], ImportDateTime ),
                        ModifiedDateTime = ImportDateTime,
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ModifiedByPersonAliasId = ImportPersonAliasId,
                        ForeignKey = rowGroupTypeKey,
                        ForeignId = groupTypeForeignId
                    };

                    // set meeting location
                    var definedValueService = new DefinedValueService( lookupContext );
                    newGroupType.LocationTypes = new List<GroupTypeLocationType>();
                    newGroupType.LocationTypes.Clear();
                    var meetingLocationType = definedValueService.Get( locationMeetingId );
                    if ( meetingLocationType != null )
                    {
                        newGroupType.LocationTypes.Add( new GroupTypeLocationType { LocationTypeValueId = meetingLocationType.Id } );
                    }

                    // set provided optional properties
                    newGroupType.TakesAttendance = ( bool ) ParseBoolOrDefault( row[GroupTypeTakesAttendance], false );
                    newGroupType.AttendanceCountsAsWeekendService = ( bool ) ParseBoolOrDefault( row[GroupTypeWeekendService], false );
                    newGroupType.ShowInGroupList = ( bool ) ParseBoolOrDefault( row[GroupTypeShowInGroupList], false );
                    newGroupType.ShowInNavigation = ( bool ) ParseBoolOrDefault( row[GroupTypeShowInNav], false );

                    // set schedule
                    var allowGroupScheduleWeekly = ( bool ) ParseBoolOrDefault( row[GroupTypeWeeklySchedule], false );
                    if ( allowGroupScheduleWeekly )
                    {
                        newGroupType.AllowedScheduleTypes = ScheduleType.Weekly;
                    }

                    var rowGroupTypePurpose = row[GroupTypePurpose];
                    if ( !string.IsNullOrWhiteSpace( rowGroupTypePurpose ) )
                    {
                        var purposeId = purposeTypeValues.Where( v => v.Value.Equals( rowGroupTypePurpose ) || v.Id.Equals( rowGroupTypePurpose.AsInteger() ) || v.Guid.ToString().ToLower().Equals( rowGroupTypePurpose.ToLower() ) )
                                .Select( v => ( int? ) v.Id ).FirstOrDefault();

                        newGroupType.GroupTypePurposeValueId = purposeId;
                    }

                    var inheritedGroupType = GroupTypeCache.Get( LoadGroupTypeId( lookupContext, row[GroupTypeInheritedGroupType] ) );
                    if ( inheritedGroupType.Id != GroupTypeCache.Get( new Guid( "8400497B-C52F-40AE-A529-3FCCB9587101" ), lookupContext ).Id )
                    {
                        newGroupType.InheritedGroupTypeId = inheritedGroupType.Id;
                    }

                    // add default role of member
                    var defaultRoleGuid = Guid.NewGuid();
                    var memberRole = new GroupTypeRole { Guid = defaultRoleGuid, Name = "Member" };
                    newGroupType.Roles.Add( memberRole );

                    // save changes each loop
                    newGroupTypeList.Add( newGroupType );

                    lookupContext.WrapTransaction( () =>
                    {
                        lookupContext.GroupTypes.AddRange( newGroupTypeList );
                        lookupContext.SaveChanges( DisableAuditing );
                    } );

                    // Set Parent Group Type
                    var rowGroupTypeParentId = row[GroupTypeParentId];
                    if ( !string.IsNullOrWhiteSpace( rowGroupTypeParentId ) )
                    {
                        var parentGroupType = ImportedGroupTypes.FirstOrDefault( t => t.ForeignKey.Equals( rowGroupTypeParentId ) );
                        var parentGroupTypeList = new List<GroupType>();
                        parentGroupTypeList.Add( parentGroupType );
                        newGroupType.ParentGroupTypes = parentGroupTypeList;
                    }

                    // Set Self Reference
                    bool selfRef;
                    TryParseBool( row[GroupTypeSelfReference], out selfRef );
                    if ( selfRef )
                    {
                        var selfReferenceList = new List<GroupType>();
                        selfReferenceList.Add( newGroupType );
                        newGroupType.ChildGroupTypes = selfReferenceList;
                    }

                    // set default role
                    newGroupType.DefaultGroupRole = newGroupType.Roles.FirstOrDefault();

                    // save changes
                    lookupContext.SaveChanges();

                    // add these new groups to the global list
                    ImportedGroupTypes.AddRange( newGroupTypeList );

                    newGroupTypeList.Clear();

                    //
                    // Keep the user informed as to what is going on and save in batches.
                    //
                    completed++;
                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} groups imported." );
                    }

                    if ( completed % ReportingNumber < 1 )
                    {
                        ReportPartialProgress();
                    }
                }
            }

            ReportProgress( 0, $"Finished group type import: {completed:N0} group types added or updated." );

            return completed;
        }

        /// <summary>
        /// Loads the polygon group data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadGroupPolygon( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var numImportedGroups = ImportedGroups.Count();
            var newGroupLocations = new Dictionary<GroupLocation, string>();
            var currentGroup = new Group();
            var coordinateString = string.Empty;
            var startCoordinate = string.Empty;
            var endCoordinate = string.Empty;
            var geographicAreaTypeId = DefinedValueCache.Get( "44990C3F-C45B-EDA3-4B65-A238A581A26F" ).Id;

            var completed = 0;

            ReportProgress( 0, $"Starting polygon group import ({numImportedGroups:N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowGroupKey = row[GroupId];
                var rowGroupId = rowGroupKey.AsType<int?>();
                var rowLat = row[Latitude];
                var rowLong = row[Longitude];

                //
                // Determine if we are still working with the same group or not.
                //
                if ( !string.IsNullOrWhiteSpace( rowGroupKey ) && rowGroupKey != currentGroup.ForeignKey )
                {
                    if ( !string.IsNullOrWhiteSpace( coordinateString ) )
                    {
                        if ( startCoordinate != endCoordinate )
                        {
                            coordinateString = $"{coordinateString}|{startCoordinate}";
                        }

                        var coords = coordinateString.Split( '|' );
                        if ( coords.Length > 3 )
                        {
                            var polygon = CreatePolygonLocation( coordinateString, row[GroupCreatedDate], rowGroupKey, rowGroupId );

                            if ( polygon != null )
                            {
                                var geographicArea = new GroupLocation
                                {
                                    LocationId = polygon.Id,
                                    IsMailingLocation = true,
                                    IsMappedLocation = true,
                                    GroupLocationTypeValueId = geographicAreaTypeId,
                                    GroupId = currentGroup.Id
                                };
                                newGroupLocations.Add( geographicArea, currentGroup.ForeignKey );
                            }
                        }
                    }

                    currentGroup = LoadGroupBasic( lookupContext, rowGroupKey, row[GroupName], row[GroupCreatedDate], row[GroupType], row[GroupParentGroupId], row[GroupActive] );

                    // reset coordinateString
                    coordinateString = string.Empty;

                    if ( !string.IsNullOrWhiteSpace( rowLat ) && !string.IsNullOrWhiteSpace( rowLong ) && rowLat.AsType<double>() != 0 && rowLong.AsType<double>() != 0 )
                    {
                        coordinateString = $"{rowLat},{rowLong}";
                        startCoordinate = $"{rowLat},{rowLong}";
                    }

                    //
                    // Set the group campus
                    //
                    var campusName = row[GroupCampus];
                    if ( !string.IsNullOrWhiteSpace( campusName ) )
                    {
                        var groupCampus = CampusList.FirstOrDefault( c => c.Name.Equals( campusName, StringComparison.OrdinalIgnoreCase )
                            || c.ShortCode.Equals( campusName, StringComparison.OrdinalIgnoreCase ) );
                        if ( groupCampus == null )
                        {
                            groupCampus = new Campus
                            {
                                IsSystem = false,
                                Name = campusName,
                                ShortCode = campusName.RemoveWhitespace(),
                                IsActive = true
                            };
                            lookupContext.Campuses.Add( groupCampus );
                            lookupContext.SaveChanges( DisableAuditing );
                            CampusList.Add( groupCampus );
                        }

                        currentGroup.CampusId = groupCampus.Id;
                    }

                    //
                    // Set the group's sorting order.
                    //
                    var groupOrder = 9999;
                    int.TryParse( row[GroupOrder], out groupOrder );
                    currentGroup.Order = groupOrder;

                    //
                    // Changes to groups need to be saved right away since one group
                    // will reference another group.
                    //
                    lookupContext.SaveChanges();

                    completed++;

                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} groups imported." );
                    }

                    if ( completed % ReportingNumber < 1 )
                    {
                        ReportPartialProgress();
                    }
                }
                else if ( rowGroupKey == currentGroup.ForeignKey && ( !string.IsNullOrWhiteSpace( rowLat ) && !string.IsNullOrWhiteSpace( rowLong ) && rowLat.AsType<double>() != 0 && rowLong.AsType<double>() != 0 ) )
                {
                    coordinateString = $"{coordinateString}|{rowLat},{rowLong}";
                    endCoordinate = $"{rowLat},{rowLong}";
                }
            }

            if ( !string.IsNullOrWhiteSpace( coordinateString ) )
            {
                if ( startCoordinate != endCoordinate )
                {
                    coordinateString = coordinateString + $"|{startCoordinate}";
                }

                var coords = coordinateString.Split( '|' );
                if ( coords.Length > 3 )
                {
                    var polygon = CreatePolygonLocation( coordinateString, currentGroup.CreatedDateTime.ToString(), currentGroup.ForeignKey, currentGroup.ForeignId );

                    if ( polygon != null )
                    {
                        var geographicArea = new GroupLocation
                        {
                            LocationId = polygon.Id,
                            IsMailingLocation = true,
                            IsMappedLocation = true,
                            GroupLocationTypeValueId = geographicAreaTypeId,
                            GroupId = currentGroup.Id
                        };
                        newGroupLocations.Add( geographicArea, currentGroup.ForeignKey );
                    }
                }
            }

            //
            // Save rows to the database
            //
            ReportProgress( 0, $"Saving {newGroupLocations.Count} polygons." );
            if ( newGroupLocations.Any() )
            {
                SaveGroupLocations( newGroupLocations );
            }

            lookupContext.SaveChanges();
            DetachAllInContext( lookupContext );
            lookupContext.Dispose();

            ReportProgress( 0, $"Finished polygon group import: {completed:N0} groups added or updated." );

            return completed;
        }

        /// <summary>
        /// Creates a new polygon location.
        /// </summary>
        /// <param name="coordinateString">String that contains the shapes. Should be formatted as: lat1,long1|lat2,long2|...</param>
        /// <param name="rowGroupCreatedDate">string to use as the CreatedDate.</param>
        /// <param name="rowGroupKey">String to use as the ForeignKey.</param>
        /// <param name="rowGroupId">Int to use as the ForeignId.</param>
        /// <returns></returns>
        private static Location CreatePolygonLocation( string coordinateString, string rowGroupCreatedDate, string rowGroupKey, int? rowGroupId )
        {
            var rockContext = new RockContext();
            var newPolygonList = new List<Location>();

            var polygon = new Location
            {
                GeoFence = DbGeography.PolygonFromText( Rock.Web.UI.Controls.GeoPicker.ConvertPolyToWellKnownText( coordinateString ), 4326 ),
                CreatedDateTime = ParseDateOrDefault( rowGroupCreatedDate, ImportDateTime ),
                ModifiedDateTime = ImportDateTime,
                CreatedByPersonAliasId = ImportPersonAliasId,
                ModifiedByPersonAliasId = ImportPersonAliasId,
                ForeignKey = rowGroupKey,
                ForeignId = rowGroupId
            };

            newPolygonList.Add( polygon );

            rockContext.WrapTransaction( () =>
            {
                rockContext.Locations.AddRange( newPolygonList );
                rockContext.SaveChanges( DisableAuditing );
            } );

            return polygon;
        }

        /// <summary>
        /// Get the Group Type Id by testing int, guid, and name.
        /// If not found, return the General Group Type Id.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        private static int LoadGroupTypeId( RockContext lookupContext, string type )
        {
            int typeId;
            var groupTypeId = -1;

            //
            // Try to figure out what group type the want, we accept Rock Group Type ID numbers,
            // GUIDs and type names.
            //
            if ( int.TryParse( type, out typeId ) )
            {
                groupTypeId = GroupTypeCache.Get( typeId, lookupContext ).Id;
            }
            else
            {
                Guid groupTypeGuid;

                if ( Guid.TryParse( type, out groupTypeGuid ) )
                {
                    groupTypeId = GroupTypeCache.Get( groupTypeGuid, lookupContext ).Id;
                }
                else
                {
                    var groupTypeByName = new GroupTypeService( lookupContext ).Queryable().AsNoTracking().FirstOrDefault( gt => gt.Name.Equals( type, StringComparison.OrdinalIgnoreCase ) );
                    if ( groupTypeByName != null )
                    {
                        groupTypeId = groupTypeByName.Id;
                    }
                    else
                    {
                        var groupTypeByKey = new GroupTypeService( lookupContext ).Queryable().AsNoTracking().FirstOrDefault( gt => gt.ForeignKey.Equals( type, StringComparison.OrdinalIgnoreCase ) );
                        if ( groupTypeByKey != null )
                        {
                            groupTypeId = groupTypeByKey.Id;
                        }
                    }
                }
            }

            //
            // Default to the "General Groups" type if we can't find what they want.
            //
            if ( groupTypeId == -1 )
            {
                groupTypeId = GroupTypeCache.Get( new Guid( "8400497B-C52F-40AE-A529-3FCCB9587101" ), lookupContext ).Id;
            }

            return groupTypeId;
        }
    }
}