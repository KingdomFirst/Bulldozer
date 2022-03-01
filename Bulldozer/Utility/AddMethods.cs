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
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Rock;
using Rock.Attribute;
using Rock.Data;
using Rock.Model;
using Rock.Security;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
using Attribute = Rock.Model.Attribute;
using Group = Rock.Model.Group;

namespace Bulldozer.Utility
{
    public static partial class Extensions
    {
        /// <summary>
        /// Add a new defined value to the Rock system.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="typeGuid">The guid of the defined type that the defined value should be added to.</param>
        /// <param name="value">The value of the new defined value.</param>
        /// <param name="guid">An optional guid to set for the newly created defined value guid.</param>
        /// <returns></returns>
        public static DefinedValueCache AddDefinedValue( RockContext rockContext, string typeGuid, string value, string guid = "" )
        {
            DefinedValueCache definedValueCache = null;
            var definedTypeGuid = typeGuid.AsGuidOrNull();
            if ( definedTypeGuid != null && !string.IsNullOrWhiteSpace( value ) )
            {
                var definedType = new DefinedTypeService( rockContext ).Get( ( Guid ) definedTypeGuid );

                var definedValue = new DefinedValue
                {
                    IsSystem = false,
                    DefinedTypeId = definedType.Id,
                    Value = value,
                    Description = "Imported with Bulldozer"
                };

                var maxOrder = definedType.DefinedValues.Max( v => ( int? ) v.Order );
                definedValue.Order = maxOrder + 1 ?? 0;

                if ( !string.IsNullOrWhiteSpace( guid ) )
                {
                    definedValue.Guid = guid.AsGuid();
                }

                DefinedTypeCache.Remove( definedType.Id );

                rockContext.DefinedValues.Add( definedValue );
                rockContext.SaveChanges( DisableAuditing );
                definedValueCache = DefinedValueCache.Get( definedValue );
            }

            return definedValueCache;
        }

        /// <summary>
        /// Finds a defined value in the Rock system.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="typeGuid">The guid of the defined type that the defined value should be in.</param>
        /// <param name="value">The value of the desired defined value.</param>
        /// <returns></returns>
        public static DefinedValueCache FindDefinedValueByTypeAndName( RockContext rockContext, Guid typeGuid, string value )
        {
            DefinedValueCache definedValueCache = null;

            var definedType = new DefinedTypeService( rockContext ).Get( ( Guid ) typeGuid );
            var definedValue = definedType.DefinedValues.FirstOrDefault( v => v.Value.Equals( value ) );
            definedValueCache = DefinedValueCache.Get( definedValue );

            return definedValueCache;
        }

        /// <summary>
        /// Adds the attribute qualifier.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="attributeId">The attribute identifier.</param>
        /// <param name="value">The value.</param>
        public static AttributeQualifier AddAttributeQualifier( RockContext rockContext, int? attributeId, string value )
        {
            AttributeQualifier valuesQualifier = null;
            if ( attributeId.HasValue && !string.IsNullOrWhiteSpace( value ) )
            {
                rockContext = rockContext ?? new RockContext();
                valuesQualifier = new AttributeQualifierService( rockContext ).GetByAttributeId( ( int ) attributeId )
                    .FirstOrDefault( q => q.Key.Equals( "values", StringComparison.OrdinalIgnoreCase ) );
                if ( valuesQualifier != null && !valuesQualifier.Value.Contains( value ) )
                {
                    valuesQualifier.Value = $"{valuesQualifier.Value},{value}";
                    rockContext.Entry( valuesQualifier ).State = EntityState.Modified;
                    rockContext.SaveChanges( DisableAuditing );
                }
            }

            return valuesQualifier;
        }

        /// <summary>
        /// Adds the device.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="deviceName">Name of the device.</param>
        /// <param name="deviceDescription">The device description.</param>
        /// <param name="deviceTypeId">The device type identifier.</param>
        /// <param name="locationId">The location identifier.</param>
        /// <param name="ipAddress">The ip address.</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="deviceForeignKey">The device foreign key.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static Device AddDevice( RockContext rockContext, string deviceName, string deviceDescription, int deviceTypeId, int? locationId,
                string ipAddress, DateTime? dateCreated, string deviceForeignKey, bool instantSave = true, int? creatorPersonAliasId = null )
        {
            var newDevice = new Device
            {
                Name = deviceName,
                Description = deviceDescription,
                DeviceTypeValueId = deviceTypeId,
                LocationId = locationId,
                IPAddress = ipAddress,
                ForeignKey = deviceForeignKey,
                ForeignId = deviceForeignKey.AsIntegerOrNull(),
                CreatedDateTime = dateCreated,
                CreatedByPersonAliasId = creatorPersonAliasId
            };

            if ( instantSave )
            {
                rockContext = rockContext ?? new RockContext();
                rockContext.Devices.Add( newDevice );
                rockContext.SaveChanges( DisableAuditing );
            }

            return newDevice;
        }

        /// <summary>
        /// Adds the occurrence.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="deviceName">Name of the device.</param>
        /// <param name="deviceDescription">The device description.</param>
        /// <param name="deviceTypeId">The device type identifier.</param>
        /// <param name="locationId">The location identifier.</param>
        /// <param name="ipAddress">The ip address.</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="deviceForeignKey">The device foreign key.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static AttendanceOccurrence AddOccurrence( RockContext rockContext, DateTime occurrenceDate, int? groupId, int? scheduleId, int? locationId,
                bool instantSave = true, int? creatorPersonAliasId = null )
        {
            var occurrence = new AttendanceOccurrence
            {
                OccurrenceDate = occurrenceDate,
                GroupId = groupId,
                ScheduleId = scheduleId,
                LocationId = locationId,
                CreatedByPersonAliasId = creatorPersonAliasId,
            };

            if ( instantSave )
            {
                rockContext = rockContext ?? new RockContext();
                rockContext.AttendanceOccurrences.Add( occurrence );
                rockContext.SaveChanges( DisableAuditing );
            }

            return occurrence;
        }

        /// <summary>
        /// Adds the account.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="fundName">Name of the fund.</param>
        /// <param name="fundDescription">The fund description.</param>
        /// <param name="accountGL">The account gl.</param>
        /// <param name="fundCampusId">The fund campus identifier.</param>
        /// <param name="parentAccountId">The parent account identifier.</param>
        /// <param name="isActive">The is active.</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="accountForeignKey">The account foreign key.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static FinancialAccount AddFinancialAccount( RockContext rockContext, string fundName, string fundDescription, string accountGL, int? fundCampusId,
            int? parentAccountId, bool? isActive, DateTime? dateCreated, string accountForeignKey, bool instantSave = true, int? creatorPersonAliasId = null, int? accountTypeValueId = null )
        {
            rockContext = rockContext ?? new RockContext();

            var account = new FinancialAccount
            {
                Name = fundName.Truncate( 50 ),
                Description = fundDescription,
                PublicName = fundName.Truncate( 50 ),
                GlCode = accountGL,
                IsTaxDeductible = true,
                IsActive = isActive ?? true,
                IsPublic = false,
                Order = 0,
                CampusId = fundCampusId,
                ParentAccountId = parentAccountId,
                CreatedDateTime = dateCreated,
                CreatedByPersonAliasId = creatorPersonAliasId,
                ForeignKey = accountForeignKey,
                ForeignId = accountForeignKey.AsIntegerOrNull(),
                AccountTypeValueId = accountTypeValueId
            };

            if ( instantSave )
            {
                rockContext.FinancialAccounts.Add( account );
                rockContext.SaveChanges( DisableAuditing );
            }

            return account;
        }

        /// <summary>
        /// Adds the named location.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="parentLocationId">The parent location identifier.</param>
        /// <param name="locationName">Name of the location.</param>
        /// <param name="locationActive">if set to <c>true</c> [location active].</param>
        /// <param name="locationCapacity">The location capacity.</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="locationForeignKey">The location foreign key.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static Location AddNamedLocation( RockContext rockContext, int? parentLocationId, string locationName, bool? locationActive,
                int? locationCapacity, DateTime? dateCreated, string locationForeignKey, bool instantSave = true, int? creatorPersonAliasId = null )
        {
            var newLocation = new Location
            {
                Name = locationName,
                IsActive = locationActive ?? true,
                ParentLocationId = parentLocationId,
                FirmRoomThreshold = locationCapacity,
                ForeignKey = locationForeignKey,
                ForeignId = locationForeignKey.AsIntegerOrNull(),
                CreatedDateTime = dateCreated,
                CreatedByPersonAliasId = creatorPersonAliasId
            };

            if ( instantSave )
            {
                rockContext = rockContext ?? new RockContext();
                rockContext.Locations.Add( newLocation );
                rockContext.SaveChanges( DisableAuditing );
            }

            return newLocation;
        }

        /// <summary>
        /// Adds the group.
        /// </summary>
        /// <param name="rockContext">todo: describe rockContext parameter on AddGroup</param>
        /// <param name="groupTypeId">The group type identifier.</param>
        /// <param name="parentGroupId">The parent group identifier.</param>
        /// <param name="groupName">Name of the group.</param>
        /// <param name="groupActive">if set to <c>true</c> [group active].</param>
        /// <param name="campusId">The campus identifier.</param>
        /// <param name="dateCreated">todo: describe dateCreated parameter on AddGroup</param>
        /// <param name="groupForeignKey">The group foreign key.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="creatorPersonAliasId">todo: describe creatorPersonAliasId parameter on AddGroup</param>
        /// <param name="scheduleId">The schedule identifier.</param>
        /// <returns></returns>
        public static Group AddGroup( RockContext rockContext, int? groupTypeId, int? parentGroupId, string groupName, bool? groupActive,
            int? campusId, DateTime? dateCreated, string groupForeignKey, bool instantSave = true, int? creatorPersonAliasId = null, int? scheduleId = null )
        {
            var newGroup = new Group
            {
                IsSystem = false,
                IsPublic = false,
                IsSecurityRole = false,
                Name = groupName,
                Description = $"{groupName} imported {RockDateTime.Now}",
                CampusId = campusId,
                ParentGroupId = parentGroupId,
                IsActive = groupActive ?? true,
                ScheduleId = scheduleId,
                CreatedDateTime = dateCreated,
                GroupTypeId = groupTypeId ?? GeneralGroupTypeId,
                ForeignKey = groupForeignKey,
                ForeignId = groupForeignKey.AsIntegerOrNull(),
                CreatedByPersonAliasId = creatorPersonAliasId
            };

            if ( instantSave )
            {
                rockContext = rockContext ?? new RockContext();
                rockContext.Groups.Add( newGroup );
                rockContext.SaveChanges( DisableAuditing );
            }

            return newGroup;
        }

        /// <summary>
        /// Builds out a Serving Group hierarchy based off the hierarchy of a specific group.
        /// </summary>
        /// <param name="rockContext">todo: describe rockContext parameter on AddGroup</param>
        /// <param name="topLevelServingGroup">The top level serving group to build the group structure under.</param>
        /// <param name="nonServingParentGroup">The parent group to copy from and use for cloning its hierarchy.</param>
        /// <param name="copyCampus">Should the campus of the nonServingParentGroup be copied to the new group structure?.</param>
        /// <param name="creatorPersonAliasId">todo: describe creatorPersonAliasId parameter on AddGroup</param>
        /// <returns></returns>
        public static List<Group> BuildParentServingGroupHierarchy( RockContext rockContext, Group topLevelServingGroup, Group nonServingParentGroup, bool copyCampus = false, int? creatorPersonAliasId = null )
        {
            var groupHierarchyToCopy = GetGroupHierarchyAscending( rockContext, nonServingParentGroup );
            var parentServingGroup = topLevelServingGroup;
            var newGroups = new List<Group>();
            foreach ( var group in groupHierarchyToCopy )
            {
                var servingGroup = new GroupService( rockContext ).Queryable().FirstOrDefault( g => g.ForeignKey == "SERVT_" + group.ForeignKey || g.ForeignKey == "SERV_" + group.ForeignKey );
                if ( servingGroup == null )
                {
                    var newGroup = new Group
                    {
                        IsSystem = false,
                        IsPublic = false,
                        IsSecurityRole = false,
                        Name = group.Name,
                        Description = $"{group.Name} imported {RockDateTime.Now}",
                        CampusId = copyCampus ? group.CampusId : null,
                        ParentGroupId = parentServingGroup.Id,
                        IsActive = group.IsActive,
                        CreatedDateTime = group.CreatedDateTime,
                        GroupTypeId = ServingTeamGroupType.Id,
                        ForeignKey = "SERV_" + group.ForeignKey,
                        CreatedByPersonAliasId = creatorPersonAliasId
                    };
                    rockContext.Groups.Add( newGroup );
                    rockContext.SaveChanges( DisableAuditing );
                    parentServingGroup = newGroup;
                    newGroups.Add( newGroup );
                }
                else
                {
                    parentServingGroup = servingGroup;
                }
            }

            return newGroups;
        }

        /// <summary>
        /// Recursive method to gather all the groups in a specific group's hierarchy from the bottom up.
        /// </summary>
        /// <param name="childGroup">Child Group to build up from.</param>
        /// <param name="groupHierarchy">Running list of groups to handle recursion.</param>
        /// <returns></returns>
        public static List<Group> GetGroupHierarchyAscending( RockContext rockContext, Group childGroup, List<Group> groupHierarchy = null )
        {
            if ( groupHierarchy == null )
            {
                groupHierarchy = new List<Group>();
            }
            groupHierarchy.Insert( 0, childGroup );
            var parentGroup = childGroup.ParentGroup;
            if ( parentGroup == null )
            {
                parentGroup = new GroupService( rockContext ).Queryable().FirstOrDefault( g => g.Id == childGroup.ParentGroupId );
            }
            if ( parentGroup != null && parentGroup.ForeignKey != "ArchivedGroups" )
            {
                GetGroupHierarchyAscending( rockContext, parentGroup, groupHierarchy );
            }
            return groupHierarchy;
        }

        /// <summary>
        /// Adds a new group type to the Rock system.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="typeDescription">The type description.</param>
        /// <param name="typeParentId">The type parent identifier.</param>
        /// <param name="inheritedGroupTypeId">The inherited group type identifier.</param>
        /// <param name="typePurposeValueId">The type purpose value identifier.</param>
        /// <param name="typeTakesAttendance">if set to <c>true</c> [type takes attendance].</param>
        /// <param name="attendanceIsWeekendService">if set to <c>true</c> [attendance is weekend service].</param>
        /// <param name="showInGroupList">if set to <c>true</c> [show in group list].</param>
        /// <param name="showInNavigation">if set to <c>true</c> [show in navigation].</param>
        /// <param name="typeOrder">todo: describe typeOrder parameter on AddGroupType</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="typeForeignKey">todo: describe typeForeignKey parameter on AddGroupType</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static GroupType AddGroupType( RockContext rockContext, string typeName, string typeDescription, int? typeParentId, int? inheritedGroupTypeId,
                    int? typePurposeValueId, bool typeTakesAttendance, bool attendanceIsWeekendService, bool showInGroupList, bool showInNavigation,
                    int typeOrder = 0, bool instantSave = true, DateTime? dateCreated = null, string typeForeignKey = null, int? creatorPersonAliasId = null )
        {
            rockContext = rockContext ?? new RockContext();

            var newGroupType = new GroupType
            {
                // set required properties (terms set by default)
                IsSystem = false,
                Name = typeName,
                Description = typeDescription,
                InheritedGroupTypeId = inheritedGroupTypeId,
                GroupTypePurposeValueId = typePurposeValueId,
                TakesAttendance = typeTakesAttendance,
                AttendanceCountsAsWeekendService = attendanceIsWeekendService,
                ShowInGroupList = showInGroupList,
                ShowInNavigation = showInNavigation,
                Order = typeOrder,
                CreatedDateTime = dateCreated,
                ModifiedDateTime = RockDateTime.Now,
                CreatedByPersonAliasId = creatorPersonAliasId,
                ModifiedByPersonAliasId = creatorPersonAliasId,
                ForeignKey = typeForeignKey,
                ForeignId = typeForeignKey.AsIntegerOrNull()
            };

            // set meeting location
            newGroupType.LocationTypes.Add( new GroupTypeLocationType { LocationTypeValueId = GroupTypeMeetingLocationId } );

            // add default role of member
            newGroupType.Roles.Add( new GroupTypeRole { Guid = Guid.NewGuid(), Name = "Member" } );

            // add parent
            if ( typeParentId.HasValue )
            {
                var parentType = new GroupTypeService( rockContext ).Get( ( int ) typeParentId );
                if ( parentType != null )
                {
                    newGroupType.ParentGroupTypes.Add( parentType );
                }
            }

            // allow children of the same type
            newGroupType.ChildGroupTypes.Add( newGroupType );

            if ( instantSave )
            {
                rockContext.GroupTypes.Add( newGroupType );
                rockContext.SaveChanges();

                newGroupType.DefaultGroupRole = newGroupType.Roles.FirstOrDefault();
                rockContext.SaveChanges();
            }

            return newGroupType;
        }

        /// <summary>
        /// Adds the named schedule.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="scheduleName">Name of the schedule.</param>
        /// <param name="iCalendarContent">Content of the i calendar.</param>
        /// <param name="dayOfWeek">The day of week.</param>
        /// <param name="timeOfDay">The time of day.</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="scheduleForeignKey">The schedule foreign key.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <param name="isActive">The active status of the schedule.</param>
        /// <returns></returns>
        public static Schedule AddNamedSchedule( RockContext rockContext, string scheduleName, string iCalendarContent, DayOfWeek? dayOfWeek,
            DateTime? timeOfDay, DateTime? dateCreated, string scheduleForeignKey, bool instantSave = true, int? creatorPersonAliasId = null, bool isActive = true )
        {
            var newSchedule = new Schedule
            {
                Name = scheduleName,
                Description = $"{scheduleName} imported {RockDateTime.Now}",
                iCalendarContent = iCalendarContent,
                WeeklyDayOfWeek = dayOfWeek,
                WeeklyTimeOfDay = timeOfDay.HasValue ? ( ( DateTime ) timeOfDay ).TimeOfDay as TimeSpan? : null,
                CreatedDateTime = dateCreated,
                ForeignKey = scheduleForeignKey,
                ForeignId = scheduleForeignKey.AsIntegerOrNull(),
                CreatedByPersonAliasId = creatorPersonAliasId,
                IsActive = isActive
            };

            if ( instantSave )
            {
                rockContext = rockContext ?? new RockContext();
                rockContext.Schedules.Add( newSchedule );
                rockContext.SaveChanges( DisableAuditing );
            }

            return newSchedule;
        }

        /// <summary>
        /// Gets the group type role.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="groupTypeId">The group type identifier.</param>
        /// <param name="roleName">Name of the role.</param>
        /// <param name="roleDescription">The role description.</param>
        /// <param name="isLeader">if set to <c>true</c> [is leader].</param>
        /// <param name="roleOrder">The role order.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="roleForeignKey">The role foreign key.</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static GroupTypeRole GetGroupTypeRole( RockContext rockContext, int groupTypeId, string roleName, string roleDescription, bool isLeader = false,
                    int roleOrder = 0, bool instantSave = true, DateTime? dateCreated = null, string roleForeignKey = null, int? creatorPersonAliasId = null )
        {
            rockContext = rockContext ?? new RockContext();

            var queryable = new GroupTypeRoleService( rockContext ).Queryable().Where( r => r.GroupTypeId == groupTypeId );

            if ( string.IsNullOrWhiteSpace( roleName ) )
            {
                return queryable.OrderByDescending( r => r.Id == r.GroupType.DefaultGroupRoleId ).ThenBy( r => r.Order ).FirstOrDefault();
            }

            // match grouptype role of
            var groupTypeRole = queryable.FirstOrDefault( r => r.Name.Equals( roleName ) );
            if ( groupTypeRole == null )
            {
                groupTypeRole = new GroupTypeRole
                {
                    IsSystem = false,
                    GroupTypeId = groupTypeId,
                    Name = roleName,
                    Description = !string.IsNullOrWhiteSpace( roleDescription )
                        ? roleDescription
                        : $"{roleName} imported {RockDateTime.Now}",
                    Order = roleOrder,
                    IsLeader = isLeader,
                    CanView = isLeader,
                    CanEdit = isLeader, // leaders should be able to edit their own groups
                    ForeignKey = roleForeignKey,
                    ForeignId = roleForeignKey.AsIntegerOrNull(),
                    CreatedDateTime = dateCreated,
                    CreatedByPersonAliasId = creatorPersonAliasId
                };

                if ( instantSave )
                {
                    rockContext.GroupTypeRoles.Add( groupTypeRole );
                    rockContext.SaveChanges();
                }
            }

            return groupTypeRole;
        }

        /// <summary>
        /// Finds an existing attribute category or adds a new one if it doesn't exist.
        /// </summary>
        /// <param name="rockContext">The RockContext to use when searching or creating categories</param>
        /// <param name="categoryEntityTypeId">The category entity type identifier.</param>
        /// <param name="categoryName">The name of the category to search for, whitespace and capitalization is ignored when searching</param>
        /// <param name="findOnly">todo: describe findOnly parameter on GetAttributeCategory</param>
        /// <param name="targetEntityTypeId">todo: describe entityTypeId parameter on GetAttributeCategory</param>
        /// <param name="importPersonAliasId">todo: describe importPeronAliasId parameter on GetAttributeCategory</param>
        public static Category GetCategory( RockContext rockContext, int categoryEntityTypeId, int? parentCategoryId, string categoryName,
            bool findOnly = false, string targetEntityQualifier = "", string targetEntityTypeId = "", int? importPersonAliasId = null )
        {
            var category = new CategoryService( rockContext ).GetByEntityTypeId( categoryEntityTypeId )
                .FirstOrDefault( c => c.EntityTypeQualifierValue == targetEntityTypeId && c.Name.ToUpper() == categoryName.ToUpper() && ( !parentCategoryId.HasValue || c.ParentCategoryId == parentCategoryId ) );
            if ( category == null && !findOnly )
            {
                category = new Category
                {
                    IsSystem = false,
                    EntityTypeId = categoryEntityTypeId,
                    EntityTypeQualifierColumn = targetEntityQualifier,
                    EntityTypeQualifierValue = targetEntityTypeId,
                    ParentCategoryId = parentCategoryId,
                    Name = categoryName,
                    Order = 0,
                    CreatedByPersonAliasId = importPersonAliasId,
                    ModifiedByPersonAliasId = importPersonAliasId
                };

                rockContext.Categories.Add( category );
                rockContext.SaveChanges( DisableAuditing );
            }

            return category;
        }

        /// <summary>
        /// Finds an existing attribute category or adds a new one if it doesn't exist.
        /// </summary>
        /// <param name="rockContext">The RockContext to use when searching or creating categories</param>
        /// <param name="categoryName">The name of the category to search for, whitespace and capitalization is ignored when searching</param>
        /// <param name="findOnly">todo: describe findOnly parameter on GetAttributeCategory</param>
        /// <param name="entityTypeId">todo: describe entityTypeId parameter on GetAttributeCategory</param>
        /// <param name="importPersonAliasId">todo: describe importPeronAliasId parameter on GetAttributeCategory</param>
        /// <returns>A reference to the Category object, attached to the lookupContext</returns>
        [Obsolete( "Use GetCategory instead with categoryEntityTypeId = AttributeEntityTypeid" )]
        public static Category GetAttributeCategory( RockContext rockContext, string categoryName, bool findOnly = false, int entityTypeId = -1, int? importPersonAliasId = null )
        {
            if ( entityTypeId == -1 )
            {
                entityTypeId = PersonEntityTypeId;
            }

            var category = new CategoryService( rockContext ).GetByEntityTypeId( AttributeEntityTypeId )
                .FirstOrDefault( c => c.EntityTypeQualifierValue == entityTypeId.ToString() && c.Name.ToUpper() == categoryName.ToUpper() );
            if ( category == null && !findOnly )
            {
                category = new Category
                {
                    IsSystem = false,
                    EntityTypeId = AttributeEntityTypeId,
                    EntityTypeQualifierColumn = "EntityTypeId",
                    EntityTypeQualifierValue = entityTypeId.ToString(),
                    Name = categoryName,
                    Order = 0,
                    CreatedByPersonAliasId = importPersonAliasId,
                    ModifiedByPersonAliasId = importPersonAliasId
                };

                rockContext.Categories.Add( category );
                rockContext.SaveChanges( DisableAuditing );
            }

            return category;
        }

        /// <summary>
        /// Tries to find an existing Attribute with the given name for an Entity Type. If the attribute is found then ensure
        /// it is a member of the specified category. If attribute must be added to the category then a save is
        /// performed instantly.
        /// </summary>
        /// <param name="rockContext">The RockContext object to work in for database access</param>
        /// <param name="categoryName">Name of the Category to assign this Attribute to, pass null for none</param>
        /// <param name="attributeName">Name of the attribute to find or create</param>
        /// <param name="entityTypeId">The Id of the Entity Type for the attribute</param>
        /// <param name="attributeForeignKey">The Foreign Key of the attribute</param>
        /// <returns>Attribute object of the found Entity Attribute</returns>
        public static Attribute FindEntityAttribute( RockContext rockContext, string categoryName, string attributeName, int entityTypeId, string attributeForeignKey = null, string attributeKey = null )
        {
            var attributeService = new AttributeService( rockContext );
            var categoryService = new CategoryService( rockContext );
            Attribute attribute = null;

            // TODO: Confirm this doesn't return a newly created attribute.

            if ( !string.IsNullOrWhiteSpace( attributeForeignKey ) )
            {
                attribute = attributeService.GetByEntityTypeId( entityTypeId ).Include( "Categories" )
                    .FirstOrDefault( a => a.ForeignKey == attributeForeignKey );
            }
            else
            {
                attribute = attributeService.GetByEntityTypeId( entityTypeId ).Include( "Categories" )
                    .FirstOrDefault( a =>
                        (
                            a.Name.Replace( " ", "" ).ToUpper() == attributeName.Replace( " ", "" ).ToUpper() ||
                            a.Key == attributeName
                        ) &&
                        (
                            ( string.IsNullOrEmpty( categoryName ) ) ||
                            ( a.Categories.Count( c => c.Name.ToUpper() == categoryName.ToUpper() ) > 0 )
                        )
                    );
            }

            if ( attribute == null && !string.IsNullOrWhiteSpace( attributeKey ) )
            {
                attribute = attributeService.GetByEntityTypeId( entityTypeId ).FirstOrDefault( a => a.Key == attributeKey );
            }

            return attribute;
        }

        /// <summary>
        /// Adds the communication.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="mediumEntityTypeId">The medium entity type identifier.</param>
        /// <param name="itemCaption">The item caption.</param>
        /// <param name="communicationText">The communication text.</param>
        /// <param name="isBulkEmail">if set to <c>true</c> [is bulk email].</param>
        /// <param name="itemStatus">The item status.</param>
        /// <param name="recipients">The recipients.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="itemForeignKey">The item foreign key.</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static Communication AddCommunication( RockContext rockContext, int mediumEntityTypeId, string itemCaption, string communicationText, bool isBulkEmail, CommunicationStatus itemStatus,
           List<CommunicationRecipient> recipients = null, bool instantSave = true, DateTime? dateCreated = null, string itemForeignKey = null, int? creatorPersonAliasId = null )
        {
            var communication = new Communication
            {
                Subject = itemCaption,
                Message = communicationText,
                FromName = string.Empty,
                FromEmail = string.Empty,
                IsBulkCommunication = isBulkEmail,
                Status = itemStatus,
                CreatedDateTime = dateCreated,
                CreatedByPersonAliasId = creatorPersonAliasId,
                SenderPersonAliasId = creatorPersonAliasId,
                ReviewerPersonAliasId = creatorPersonAliasId,
                ForeignKey = itemForeignKey,
                ForeignId = itemForeignKey.AsIntegerOrNull(),
                Recipients = recipients
            };

            if ( instantSave )
            {
                rockContext = rockContext ?? new RockContext();
                rockContext.Communications.Add( communication );
                rockContext.SaveChanges();
            }

            return communication;
        }

        /// <summary>
        /// Adds the prayer request.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="categoryId">The category identifier.</param>
        /// <param name="requestorAliasId">The requester alias identifier.</param>
        /// <param name="requestFirst">The request first.</param>
        /// <param name="requestLast">The request last.</param>
        /// <param name="requestEmail">The request email.</param>
        /// <param name="requestText">The request text.</param>
        /// <param name="requestAnswer">The request answer.</param>
        /// <param name="isActive">if set to <c>true</c> [is active].</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="dateApproved">The date approved.</param>
        /// <param name="itemForeignKey">The item foreign key.</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static PrayerRequest AddPrayerRequest( RockContext rockContext, int? categoryId, int requestorAliasId, string requestFirst, string requestLast,
            string requestEmail, string requestText, string requestAnswer, bool isActive, bool instantSave = true, DateTime? dateCreated = null, DateTime? dateApproved = null,
            string itemForeignKey = null, int? creatorPersonAliasId = null )
        {
            if ( string.IsNullOrWhiteSpace( requestFirst ) || string.IsNullOrWhiteSpace( requestText ) )
            {
                return null;
            }

            var request = new PrayerRequest
            {
                CategoryId = categoryId,
                RequestedByPersonAliasId = requestorAliasId,
                FirstName = requestFirst,
                LastName = requestLast,
                IsActive = isActive,
                IsApproved = true,
                Text = requestText,
                EnteredDateTime = dateCreated ?? Bulldozer.BulldozerComponent.ImportDateTime,
                ExpirationDate = ( dateCreated ?? Bulldozer.BulldozerComponent.ImportDateTime ).AddDays( 14 ),
                CreatedDateTime = dateCreated,
                ApprovedOnDateTime = dateApproved,
                CreatedByPersonAliasId = creatorPersonAliasId,
                ApprovedByPersonAliasId = creatorPersonAliasId,
                ForeignKey = itemForeignKey,
                ForeignId = itemForeignKey.AsIntegerOrNull()
            };

            if ( instantSave )
            {
                rockContext = rockContext ?? new RockContext();
                rockContext.PrayerRequests.Add( request );
                rockContext.SaveChanges();
            }

            return request;
        }

        /// <summary>
        /// Adds the entity note and the note type if it doesn't exist.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="noteEntityTypeId">The note entity type identifier.</param>
        /// <param name="noteEntityId">The note entity identifier.</param>
        /// <param name="noteCaption">The note caption.</param>
        /// <param name="noteText">The note text.</param>
        /// <param name="isAlert">if set to <c>true</c> [is alert].</param>
        /// <param name="isPrivate">if set to <c>true</c> [is private].</param>
        /// <param name="noteTypeName">Name of the note type.</param>
        /// <param name="noteTypeId">The note type identifier.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="noteCreated">todo: describe noteCreated parameter on AddEntityNote</param>
        /// <param name="noteForeignKey">todo: describe noteForeignKey parameter on AddEntityNote</param>
        /// <param name="creatorPersonAliasId">The import person alias identifier.</param>
        /// <returns></returns>
        public static Note AddEntityNote( RockContext rockContext, int noteEntityTypeId, int noteEntityId, string noteCaption, string noteText, bool isAlert, bool isPrivate,
            string noteTypeName, int? noteTypeId = null, bool instantSave = true, DateTime? noteCreated = null, string noteForeignKey = null, int? creatorPersonAliasId = null )
        {
            // ensure we have enough information to create a note/notetype
            if ( noteEntityTypeId <= 0 || noteEntityId <= 0 || ( noteTypeId == null && string.IsNullOrEmpty( noteTypeName ) ) )
            {
                return null;
            }

            rockContext = rockContext ?? new RockContext();

            // get the note type id (create if it doesn't exist)
            noteTypeId = noteTypeId ?? new NoteTypeService( rockContext ).Get( noteEntityTypeId, noteTypeName ).Id;

            // replace special HTML characters that destroy Rock
            noteText = Regex.Replace( noteText, @"\t|\&nbsp;", " " );
            noteText = noteText.Replace( "&lt;", "<" );
            noteText = noteText.Replace( "&gt;", ">" );
            noteText = noteText.Replace( "&amp;", "&" );
            noteText = noteText.Replace( "&quot;", @"""" );
            noteText = noteText.Replace( "&#45;", "-" );
            noteText = noteText.Replace( "&#x0D", string.Empty );

            // create the note on this person
            var note = new Note
            {
                IsSystem = false,
                IsAlert = isAlert,
                IsPrivateNote = isPrivate,
                NoteTypeId = ( int ) noteTypeId,
                EntityId = noteEntityId,
                Caption = noteCaption,
                Text = noteText.Trim(),
                ForeignKey = noteForeignKey,
                ForeignId = noteForeignKey.AsIntegerOrNull(),
                CreatedDateTime = noteCreated,
                CreatedByPersonAliasId = creatorPersonAliasId
            };

            if ( instantSave )
            {
                rockContext.Notes.Add( note );
                rockContext.SaveChanges( DisableAuditing );
            }

            return note;
        }

        /// <summary>
        /// Add an Attribute with the given category and attribute name for supplied Entity Type.
        /// </summary>
        /// <param name="rockContext">The RockContext object to work in for database access</param>
        /// <param name="entityTypeId">The Id of the Entity Type this attribute is for</param>
        /// <param name="entityTypeQualifierName">If a qualifier name is needed supply, otherwise use string.Empty</param>
        /// <param name="entityTypeQualifierValue">If a qualifier value is needed supply, otherwise use string.Empty</param>
        /// <param name="foreignKey">String matching an existing ForeignKey, otherwise use null</param>
        /// <param name="categoryName">Name of the Category to assign this Attribute to, pass null for none</param>
        /// <param name="attributeName">Name of the attribute to find or create</param>
        /// <param name="key">Attribute key to use, if not provided the attributeName without any whitespace is used</param>
        /// <param name="fieldTypeId">todo: describe fieldTypeId parameter on AddEntityAttribute</param>
        /// <param name="instantSave">If true always save changes before returning, otherwise only save when absolutely necessary</param>
        /// <param name="definedTypeForeignId">Used to determine if a Defined Type should be created from scratch or link to an existing, imported Defined Type</param>
        /// <param name="definedTypeForeignKey">Used to determine if a Defined Type should be created from scratch or link to an existing, imported Defined Type</param>
        /// <param name="importPersonAliasId">todo: describe importPersonAliasId parameter on AddEntityAttribute</param>
        /// <returns>
        /// Newly created Entity Attribute
        /// </returns>
        public static Attribute AddEntityAttribute( RockContext rockContext, int entityTypeId, string entityTypeQualifierName, string entityTypeQualifierValue, string foreignKey,
            string categoryName, string attributeName, string key, int fieldTypeId, bool instantSave = true, int? definedTypeForeignId = null, string definedTypeForeignKey = null
            , int? importPersonAliasId = null, string attributeTypeString = "" )
        {
            rockContext = rockContext ?? new RockContext();
            AttributeQualifier attributeQualifier;
            Attribute attribute;
            var newAttribute = true;

            //
            // Get a reference to the existing attribute if there is one.
            //
            attribute = FindEntityAttribute( rockContext, categoryName, attributeName, entityTypeId, foreignKey, key );
            if ( attribute != null )
            {
                newAttribute = false;
            }

            //
            // If no attribute has been found, create a new one.
            if ( attribute == null && fieldTypeId != -1 )
            {
                attribute = new Attribute
                {
                    Name = attributeName,
                    FieldTypeId = fieldTypeId,
                    EntityTypeId = entityTypeId,
                    EntityTypeQualifierColumn = entityTypeQualifierName,
                    EntityTypeQualifierValue = entityTypeQualifierValue,
                    DefaultValue = string.Empty,
                    IsMultiValue = false,
                    IsGridColumn = false,
                    IsRequired = false,
                    Order = 0,
                    CreatedByPersonAliasId = importPersonAliasId,
                    ModifiedByPersonAliasId = importPersonAliasId,
                    ForeignId = foreignKey.AsIntegerOrNull(),
                    ForeignKey = foreignKey
                };

                if ( !string.IsNullOrEmpty( key ) )
                {
                    attribute.Key = key;
                }
                else
                {
                    if ( !string.IsNullOrEmpty( categoryName ) )
                    {
                        attribute.Key = $"{categoryName.RemoveWhitespace()}_{attributeName.RemoveWhitespace()}";
                    }
                    else
                    {
                        attribute.Key = attributeName.RemoveWhitespace();
                    }
                }

                var attributeQualifiers = new List<AttributeQualifier>();

                // Do specific value type settings.
                if ( fieldTypeId == DateFieldTypeId )
                {
                    attribute.Description = attributeName + " Date created by import";

                    // Add date attribute qualifiers
                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "format",
                        Value = "",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );

                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "displaydiff",
                        Value = "false",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );

                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "displaycurrentoption",
                        Value = "false",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );
                }
                else if ( fieldTypeId == BooleanFieldTypeId )
                {
                    attribute.Description = attributeName + " Boolean created by import";

                    //
                    // Add boolean attribute qualifiers
                    //
                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "truetext",
                        Value = "Yes",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );

                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "falsetext",
                        Value = "No",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );
                }
                else if ( fieldTypeId == DefinedValueFieldTypeId || fieldTypeId == ValueListFieldTypeId )
                {
                    var useDefinedValue = false;

                    if ( fieldTypeId == DefinedValueFieldTypeId || attributeTypeString == "VL" )
                    {
                        useDefinedValue = true;
                    }

                    if ( useDefinedValue )
                    {
                        var typeService = new DefinedTypeService( rockContext );
                        DefinedType definedType = null;

                        // Check for the defined type by the original name only, id, or key.
                        var attrNameShort = attributeName.Left( 87 );
                        var definedTypeExists = typeService.Queryable().Any( t => t.Name.Equals( attrNameShort + " Defined Type" )
                            || ( definedTypeForeignId.HasValue && t.ForeignId.HasValue && t.ForeignId == definedTypeForeignId )
                            || ( !( definedTypeForeignKey == null || definedTypeForeignKey.Trim() == string.Empty ) && !( t.ForeignKey == null || t.ForeignKey.Trim() == string.Empty ) && t.ForeignKey.Equals( definedTypeForeignKey, StringComparison.OrdinalIgnoreCase ) )
                            );

                        if ( !definedTypeExists )
                        {
                            definedType = new DefinedType
                            {
                                IsSystem = false,
                                Order = 0,
                                FieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.TEXT ).Id,
                                Name = attrNameShort + " Defined Type",
                                Description = attributeName + " Defined Type created by import",
                                ForeignId = definedTypeForeignId,
                                ForeignKey = definedTypeForeignKey
                            };

                            typeService.Add( definedType );
                            rockContext.SaveChanges();
                        }
                        else
                        {
                            definedType = typeService.Queryable().FirstOrDefault( t => t.Name.Equals( attrNameShort + " Defined Type" ) || ( t.ForeignId != null && t.ForeignId == definedTypeForeignId ) || ( !( t.ForeignKey == null || t.ForeignKey.Trim() == string.Empty ) && t.ForeignKey == definedTypeForeignKey ) );
                        }

                        attribute.Description = attributeName + " Defined Type created by import";

                        //
                        // Add defined value attribute qualifier with Id
                        //
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "definedtype",
                            Value = definedType.Id.ToString(),
                            IsSystem = false
                        };

                        attribute.AttributeQualifiers.Add( attributeQualifier );
                    }
                    else
                    {
                        //
                        // Add defined value attribute qualifier
                        //
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "definedtype",
                            Value = "",
                            IsSystem = false
                        };

                        attribute.AttributeQualifiers.Add( attributeQualifier );
                    }

                    if ( fieldTypeId == DefinedValueFieldTypeId )
                    {
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "allowmultiple",
                            Value = attributeTypeString == "VM" ? "True" : "False",
                            IsSystem = false
                        };

                        attribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "displaydescription",
                            Value = "false",
                            IsSystem = false
                        };

                        attribute.AttributeQualifiers.Add( attributeQualifier );
                    }
                    else if ( fieldTypeId == ValueListFieldTypeId )
                    {
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "customvalues",
                            Value = "",
                            IsSystem = false
                        };

                        attribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "valueprompt",
                            Value = "",
                            IsSystem = false
                        };

                        attribute.AttributeQualifiers.Add( attributeQualifier );
                    }
                }
                else if ( fieldTypeId == SingleSelectFieldTypeId )
                {
                    attribute.Description = attributeName + " Single Select created by import";
                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "values",
                        Value = "Pass,Fail",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );

                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "fieldtype",
                        Value = "ddl",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );
                }
                else if ( fieldTypeId == URLLinkFieldTypeId )
                {
                    attribute.Description = attributeName + " URL Link created by import";

                    //
                    // Add URL Link attribute qualifiers
                    //
                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "ShouldAlwaysShowCondensed",
                        Value = "False",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );

                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "ShouldRequireTrailingForwardSlash",
                        Value = "False",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );
                }
                else if ( fieldTypeId == HTMLFieldTypeId )
                {
                    attribute.Description = attributeName + " HTML created by import";

                    //
                    // Add HTLML attribute qualifiers
                    //
                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "documentfolderroot",
                        Value = string.Empty,
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );

                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "imagefolderroot",
                        Value = string.Empty,
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );

                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "toolbar",
                        Value = "Light",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );

                    attributeQualifier = new AttributeQualifier
                    {
                        Key = "userspecificroot",
                        Value = "False",
                        IsSystem = false
                    };

                    attribute.AttributeQualifiers.Add( attributeQualifier );
                }
                else
                {
                    attribute.Description = attributeName + " created by import";
                }
            }

            // Create a reference to Category if they provided a name.
            if ( !string.IsNullOrEmpty( categoryName ) )
            {
                var category = GetCategory( rockContext, AttributeEntityTypeId, null, categoryName, true, "EntityTypeId", entityTypeId.ToString() );
                if ( category == null )
                {
                    category = GetCategory( rockContext, AttributeEntityTypeId, null, categoryName, false, "EntityTypeId", entityTypeId.ToString() );

                    // if new person attribute category, add to extended attributes
                    if ( entityTypeId.Equals( PersonEntityTypeId ) )
                    {
                        var blockGuid = Guid.NewGuid().ToString();
                        var BulldozerBlock = new Migration();
                        Migration.AddBlock( "1C737278-4CBA-404B-B6B3-E3F0E05AB5FE", "", "D70A59DC-16BE-43BE-9880-59598FA7A94C", category.Name, "SectionB1", "", "", 0, blockGuid );
                        Migration.AddBlockAttributeValue( blockGuid, "EC43CF32-3BDF-4544-8B6A-CE9208DD7C81", category.Guid.ToString() );
                    }
                }

                // ensure it is part of the Attribute's categories.
                if ( !attribute.Categories.Any( c => c.Id == category.Id ) )
                {
                    attribute.Categories.Add( category );
                }
            }

            if ( !newAttribute && string.IsNullOrWhiteSpace( attribute.ForeignKey ) && !string.IsNullOrWhiteSpace( foreignKey ) )
            {
                attribute.ForeignKey = foreignKey;
                attribute.ForeignId = foreignKey.AsIntegerOrNull();
            }

            if ( instantSave )
            {
                if ( newAttribute )
                {
                    rockContext.Attributes.Add( attribute );
                }
                rockContext.SaveChanges( DisableAuditing );
            }

            return attribute;
        }

        /// <summary>
        /// Add, or update, an entity's attribute value to the specified value and optionally create the history information.
        /// This method does not save changes automatically. You must call SaveChanges() on your context when you are
        /// ready to save all attribute values that have been added.
        /// </summary>
        /// <param name="rockContext">The RockContext object to work in for database access</param>
        /// <param name="attribute">The attribute of the value to set</param>
        /// <param name="entity">The Entity for which the attribute is being saved</param>
        /// <param name="value">The string-value to set the attribute to, must be parseable into the target type</param>
        /// <param name="changes">List to place any changes string into, or null. If null and instantSave is true then the History entry is saved instantly</param>
        /// <param name="csv">Bool to indicate this call was made via CSV maps. Important for how the save is processed.</param>
        /// <returns>true if the attribute value was successfully coerced into the target type</returns>
        public static bool AddEntityAttributeValue( RockContext rockContext, Attribute attribute, IHasAttributes entity, string value, List<string> changes = null, bool csv = false, string foreignKey = null, bool allowMultiple = false )
        {
            rockContext = rockContext ?? new RockContext();
            string newValue = null;

            //
            // Determine the field type and coerce the value into that type.
            //
            if ( attribute.FieldTypeId == DateFieldTypeId )
            {
                var dateValue = ParseDateOrDefault( value, null );
                if ( dateValue != null && dateValue != DefaultDateTime && dateValue != DefaultSQLDateTime )
                {
                    newValue = ( ( DateTime ) dateValue ).ToString( "s" );
                }
            }
            else if ( attribute.FieldTypeId == BooleanFieldTypeId )
            {
                var boolValue = ParseBoolOrDefault( value, null );
                if ( boolValue != null )
                {
                    newValue = ( ( bool ) boolValue ).ToString();
                }
            }
            else if ( attribute.FieldTypeId == DefinedValueFieldTypeId )
            {
                Guid definedValueGuid;
                int definedTypeId;

                definedTypeId = int.Parse( attribute.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value );
                var attributeValueTypes = DefinedTypeCache.Get( definedTypeId, rockContext );

                if ( allowMultiple )
                {
                    //
                    // Check for multiple and walk the loop
                    //
                    var valueList = new List<string>();
                    var values = value.Split( new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries ).ToList();

                    foreach ( var v in values )
                    {
                        //
                        // Add the defined value if it doesn't exist.
                        //
                        var definedValueExists = attributeValueTypes.DefinedValues.Any( a => a.Value.Equals( v ) );
                        if ( !definedValueExists )
                        {
                            var newDefinedValue = new DefinedValue
                            {
                                DefinedTypeId = attributeValueTypes.Id,
                                Value = v,
                                Order = 0
                            };

                            DefinedTypeCache.Remove( attributeValueTypes.Id );

                            rockContext.DefinedValues.Add( newDefinedValue );
                            rockContext.SaveChanges( DisableAuditing );

                            valueList.Add( newDefinedValue.Guid.ToString().ToUpper() );
                        }
                        else
                        {
                            valueList.Add( attributeValueTypes.DefinedValues.FirstOrDefault(a => a.Value.Equals( v ) ).Guid.ToString().ToUpper() );
                        }
                    }

                    //
                    // Convert list of Guids to single comma delimited string
                    //
                    newValue = valueList.AsDelimited( "," );
                }
                else
                { 
                    //
                    // Add the defined value if it doesn't exist.
                    //
                    var attributeExists = attributeValueTypes.DefinedValues.Any( a => a.Value.Equals( value ) );
                    if ( !attributeExists )
                    {
                        var newDefinedValue = new DefinedValue
                        {
                            DefinedTypeId = attributeValueTypes.Id,
                            Value = value,
                            Order = 0
                        };

                        DefinedTypeCache.Remove( attributeValueTypes.Id );

                        rockContext.DefinedValues.Add( newDefinedValue );
                        rockContext.SaveChanges( DisableAuditing );

                        definedValueGuid = newDefinedValue.Guid;
                    }
                    else
                    {
                        definedValueGuid = attributeValueTypes.DefinedValues.FirstOrDefault( a => a.Value.Equals( value ) ).Guid;
                    }

                    newValue = definedValueGuid.ToString().ToUpper();
                }
            }
            else if ( attribute.FieldTypeId == ValueListFieldTypeId )
            {
                int definedTypeId;

                definedTypeId = attribute.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value.AsInteger();
                var attributeValueTypes = DefinedTypeCache.Get( definedTypeId, rockContext );

                //
                // Check for multiple and walk the loop
                //
                var valueList = new List<string>();
                var values = value.Split( new char[] { '^' }, StringSplitOptions.RemoveEmptyEntries ).ToList();
                foreach ( var v in values )
                {
                    if ( definedTypeId > 0 )
                    {
                        //
                        // Add the defined value if it doesn't exist.
                        //
                        var attributeExists = attributeValueTypes.DefinedValues.Any( a => a.Value.Equals( v ) );
                        if ( !attributeExists )
                        {
                            var newDefinedValue = new DefinedValue
                            {
                                DefinedTypeId = attributeValueTypes.Id,
                                Value = v,
                                Order = 0
                            };

                            DefinedTypeCache.Remove( attributeValueTypes.Id );

                            rockContext.DefinedValues.Add( newDefinedValue );
                            rockContext.SaveChanges( DisableAuditing );

                            valueList.Add( newDefinedValue.Id.ToString() );
                        }
                        else
                        {
                            valueList.Add( attributeValueTypes.DefinedValues.FirstOrDefault( a => a.Value.Equals( v ) ).Id.ToString() );
                        }
                    }
                    else
                    {
                        valueList.Add( v );
                    }
                }

                //
                // Convert list of Ids to single pipe delimited string
                //
                newValue = valueList.AsDelimited( "|", "|" );
            }
            else if ( attribute.FieldTypeId == EncryptedTextFieldTypeId )
            {
                newValue = Encryption.EncryptString( value );
            }
            else
            {
                newValue = value;
            }

            // set the value on the entity
            if ( !string.IsNullOrWhiteSpace( newValue ) )
            {
                if ( entity.Id > 0 && csv )
                {
                    AttributeValue attributeValue = null;
                    var attributeValueService = new AttributeValueService( rockContext );

                    attributeValue = rockContext.AttributeValues.Local.AsQueryable().FirstOrDefault( av => av.AttributeId == attribute.Id && av.EntityId == entity.Id );
                    if ( attributeValue == null )
                    {
                        attributeValue = attributeValueService.GetByAttributeIdAndEntityId( attribute.Id, entity.Id );
                    }

                    if ( attributeValue == null )
                    {
                        attributeValue = new AttributeValue
                        {
                            EntityId = entity.Id,
                            AttributeId = attribute.Id
                        };

                        attributeValueService.Add( attributeValue );
                    }
                    var originalValue = attributeValue.Value;
                    if ( originalValue != newValue )
                    {
                        attributeValue.Value = newValue;
                        attributeValue.ForeignKey = foreignKey;
                        attributeValue.ForeignId = foreignKey.AsType<int?>();
                    }
                }
                else
                {
                    if ( entity.Attributes == null )
                    {
                        entity.LoadAttributes();
                    }

                    if ( !entity.Attributes.ContainsKey( attribute.Key ) )
                    {
                        entity.Attributes.Add( attribute.Key, AttributeCache.Get( attribute.Id, rockContext ) );
                    }

                    if ( !entity.AttributeValues.ContainsKey( attribute.Key ) )
                    {
                        entity.AttributeValues.Add( attribute.Key, new AttributeValueCache
                        {
                            AttributeId = attribute.Id,
                            Value = newValue
                        } );
                    }
                    else
                    {
                        var avc = entity.AttributeValues[attribute.Key];
                        avc.Value = newValue;
                    }
                }
            }
            else
            {
                return false;
            }

            // removed old code, see prior commit for history
            // https://repo.kingdomfirstsolutions.com/KFS/Bulldozer/blob/c42603752c0243c68c700512491599aa03b825fd/Bulldozer/Utility/AddMethods.cs#L956

            return true;
        }

        /// <summary>
        /// Returns a new Attribute Value for the provided information.
        /// </summary>
        /// <param name="rockContext">The RockContext object to work in for database access</param>
        /// <param name="attribute">The attribute of the value to set</param>
        /// <param name="entity">The Entity for which the attribute is being saved</param>
        /// <param name="value">The string-value to set the attribute to, must be parseable into the target type</param>
        /// <param name="foreignKey">The string-value to set the attribute foreign key to</param>
        /// <returns>true if the attribute value was successfully coerced into the target type</returns>
        public static AttributeValue CreateEntityAttributeValue( RockContext rockContext, Attribute attribute, IHasAttributes entity, string value, string foreignKey = null )
        {
            AttributeValue attributeValue = null;

            rockContext = rockContext ?? new RockContext();

            string newValue = null;

            //
            // Determine the field type and coerce the value into that type.
            //
            if ( attribute.FieldTypeId == DateFieldTypeId )
            {
                var dateValue = ParseDateOrDefault( value, null );
                if ( dateValue != null && dateValue != DefaultDateTime && dateValue != DefaultSQLDateTime )
                {
                    newValue = ( ( DateTime ) dateValue ).ToString( "s" );
                }
            }
            else if ( attribute.FieldTypeId == BooleanFieldTypeId )
            {
                var boolValue = ParseBoolOrDefault( value, null );
                if ( boolValue != null )
                {
                    newValue = ( ( bool ) boolValue ).ToString();
                }
            }
            else if ( attribute.FieldTypeId == DefinedValueFieldTypeId )
            {
                Guid definedValueGuid;
                var definedTypeId = int.Parse( attribute.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value );
                var attributeDVType = DefinedTypeCache.Get( definedTypeId );

                //
                // Add the defined value if it doesn't exist.
                //
                var attributeDefinedValue = FindDefinedValueByTypeAndName( new RockContext(), attributeDVType.Guid, value );
                if ( attributeDefinedValue == null )
                {
                    attributeDefinedValue = AddDefinedValue( new RockContext(), attributeDVType.Guid.ToString(), value );
                    //var dvDefinedType = new DefinedTypeService( rockContext ).Get( attributeDefinedValue.DefinedTypeId );
                    //dvDefinedType.UpdateCache( EntityState.Detached, rockContext );
                }

                definedValueGuid = attributeDefinedValue.Guid;
                newValue = definedValueGuid.ToString().ToUpper();
            }
            else if ( attribute.FieldTypeId == ValueListFieldTypeId )
            {
                int definedTypeId;

                definedTypeId = attribute.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value.AsInteger();
                var attributeValueTypes = DefinedTypeCache.Get( definedTypeId, rockContext );

                var dvRockContext = new RockContext();
                dvRockContext.Configuration.AutoDetectChangesEnabled = false;

                //
                // Check for multiple and walk the loop
                //
                var valueList = new List<string>();
                var values = value.Split( new char[] { '^' }, StringSplitOptions.RemoveEmptyEntries ).ToList();
                foreach ( var v in values )
                {
                    if ( definedTypeId > 0 )
                    {
                        //
                        // Add the defined value if it doesn't exist.
                        //
                        var attributeExists = attributeValueTypes.DefinedValues.Any( a => a.Value.Equals( v ) );
                        if ( !attributeExists )
                        {
                            var newDefinedValue = new DefinedValue
                            {
                                DefinedTypeId = attributeValueTypes.Id,
                                Value = v,
                                Order = 0
                            };

                            DefinedTypeCache.Remove( attributeValueTypes.Id );

                            dvRockContext.DefinedValues.Add( newDefinedValue );
                            dvRockContext.SaveChanges( DisableAuditing );

                            valueList.Add( newDefinedValue.Id.ToString() );
                        }
                        else
                        {
                            valueList.Add( attributeValueTypes.DefinedValues.FirstOrDefault( a => a.Value.Equals( v ) ).Id.ToString() );
                        }
                    }
                    else
                    {
                        valueList.Add( v );
                    }
                }

                //
                // Convert list of Ids to single pipe delimited string
                //
                newValue = valueList.AsDelimited( "|", "|" );
            }
            else if ( attribute.FieldTypeId == EncryptedTextFieldTypeId )
            {
                newValue = Encryption.EncryptString( value );
            }
            else
            {
                newValue = value;
            }

            // set the value on the entity
            if ( !string.IsNullOrWhiteSpace( newValue ) )
            {
                if ( entity.Id > 0 )
                {
                    var attributeValueService = new AttributeValueService( rockContext );

                    attributeValue = rockContext.AttributeValues.Local.AsQueryable().FirstOrDefault( av => av.AttributeId == attribute.Id && av.EntityId == entity.Id );
                    if ( attributeValue == null )
                    {
                        attributeValue = attributeValueService.GetByAttributeIdAndEntityId( attribute.Id, entity.Id );
                    }

                    if ( attributeValue == null )
                    {
                        attributeValue = new AttributeValue
                        {
                            EntityId = entity.Id,
                            AttributeId = attribute.Id
                        };

                        attributeValueService.Add( attributeValue );
                    }
                    var originalValue = attributeValue.Value;
                    if ( originalValue != newValue )
                    {
                        attributeValue.Value = newValue;
                        attributeValue.ForeignKey = foreignKey;
                        attributeValue.ForeignId = foreignKey.AsType<int?>();
                    }
                    else
                    {
                        attributeValue = null;
                    }
                }
            }

            return attributeValue;
        }

        /// <summary>
        /// Adds the user login.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="authProviderTypeId">The authentication provider type identifier.</param>
        /// <param name="personId">The person identifier.</param>
        /// <param name="username">The username.</param>
        /// <param name="password">The password.</param>
        /// <param name="isConfirmed">The is confirmed.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <param name="userCreated">The user created.</param>
        /// <param name="userForeignKey">The user foreign key.</param>
        /// <param name="creatorPersonAliasId">The creator person alias identifier.</param>
        /// <returns></returns>
        public static UserLogin AddUserLogin( RockContext rockContext, int? authProviderTypeId, int personId, string username, string password,
            bool? isConfirmed = true, bool instantSave = true, DateTime? userCreated = null, string userForeignKey = null, int? creatorPersonAliasId = null )
        {
            rockContext = rockContext ?? new RockContext();

            // Make sure we can create a valid userlogin
            if ( string.IsNullOrWhiteSpace( username ) || !authProviderTypeId.HasValue || rockContext.UserLogins.Any( u => u.UserName.Equals( username, StringComparison.OrdinalIgnoreCase ) ) )
            {
                return null;
            }

            var userLogin = new UserLogin
            {
                UserName = username,
                Password = password,
                EntityTypeId = authProviderTypeId,
                PersonId = personId,
                IsConfirmed = isConfirmed,
                ForeignKey = userForeignKey,
                ForeignId = userForeignKey.AsIntegerOrNull(),
                CreatedDateTime = userCreated,
                CreatedByPersonAliasId = creatorPersonAliasId
            };

            if ( instantSave )
            {
                rockContext.UserLogins.Add( userLogin );
                rockContext.SaveChanges( DisableAuditing );
            }

            return userLogin;
        }

        /// <summary>
        /// Add a new group role to the system.
        /// </summary>
        /// <param name="rockContext"></param>
        /// <param name="type">The GUID of the group type to add this role to.</param>
        /// <param name="value">The value of the new role.</param>
        public static int AddGroupRole( RockContext rockContext, string type, string value )
        {
            var groupTypeRoleService = new GroupTypeRoleService( rockContext );
            var groupTypeRole = new GroupTypeRole();
            var typeId = GroupTypeCache.Get( new Guid( type ), rockContext ).Id;

            groupTypeRole.GroupTypeId = typeId;
            groupTypeRole.IsSystem = false;

            var orders = groupTypeRoleService.Queryable()
                .Where( g => g.GroupTypeId == typeId )
                .Select( g => g.Order )
                .ToList();
            groupTypeRole.Order = orders.Any() ? orders.Max() + 1 : 0;

            groupTypeRole.Name = value;
            groupTypeRole.Description = "Imported with Bulldozer";

            GroupTypeCache.Remove( typeId );

            groupTypeRoleService.Add( groupTypeRole );
            rockContext.SaveChanges( DisableAuditing );

            return groupTypeRole.Id;
        }

        /// <summary>
        /// Gets the campus identifier.
        /// </summary>
        /// <param name="property">Name of the property.</param>
        /// <param name="includeCampusName">if set to <c>true</c> [include campus name].</param>
        /// <param name="direction">The direction, default is begins with.</param>
        /// <returns></returns>
        public static int? GetCampusId( string property, bool includeCampusName = true, SearchDirection direction = SearchDirection.Begins, string possibleCampusName = null )
        {
            int? campusId = null;
            if ( !string.IsNullOrWhiteSpace( possibleCampusName ) )
            {
                var campus = CampusList.AsQueryable().FirstOrDefault( c => c.ShortCode == possibleCampusName
                        || ( includeCampusName && c.Name == possibleCampusName ) );
                if ( campus != null )
                {
                    campusId = campus.Id;
                }
            }
            if ( campusId == null && !string.IsNullOrWhiteSpace( property ) )
            {
                var queryable = CampusList.AsQueryable();

                if ( direction == SearchDirection.Begins )
                {
                    queryable = queryable.Where( c => property.StartsWith( c.ShortCode, StringComparison.OrdinalIgnoreCase )
                        || ( includeCampusName && property.StartsWith( c.Name, StringComparison.OrdinalIgnoreCase ) ) );
                }
                else
                {
                    queryable = queryable.Where( c => property.EndsWith( c.ShortCode, StringComparison.OrdinalIgnoreCase )
                        || ( includeCampusName && property.EndsWith( c.Name, StringComparison.OrdinalIgnoreCase ) ) );
                }

                campusId = queryable.Select( c => ( int? ) c.Id ).FirstOrDefault();
            }

            return campusId;
        }

        /// <summary>
        /// Strips the prefix from the text value.
        /// </summary>
        /// <param name="textValue">The text value.</param>
        /// <param name="campusId">The campus identifier.</param>
        /// <returns></returns>
        public static string StripPrefix( string textValue, int? campusId )
        {
            var fixedValue = string.Empty;
            if ( !string.IsNullOrWhiteSpace( textValue ) && campusId.HasValue )
            {
                // Main Campus -> Main Campus
                // Main -> MAIN
                // Main-Baptized -> MAIN
                var campus = CampusList.FirstOrDefault( c => c.Id == campusId );
                textValue = textValue.StartsWith( campus.Name, StringComparison.OrdinalIgnoreCase ) ? textValue.Substring( campus.Name.Length ) : textValue;
                textValue = textValue.StartsWith( campus.ShortCode, StringComparison.OrdinalIgnoreCase ) ? textValue.Substring( campus.ShortCode.Length ) : textValue;
            }

            // strip the prefix including delimiters
            fixedValue = textValue.IndexOfAny( ValidDelimiters ) >= 0
                ? textValue.Substring( textValue.IndexOfAny( ValidDelimiters ) + 1 )
                : textValue;

            return CultureInfo.CurrentCulture.TextInfo.ToTitleCase( fixedValue.Trim() );
        }

        /// <summary>
        /// Strips the suffix from the text value.
        /// </summary>
        /// <param name="textValue">The text value.</param>
        /// <param name="campusId">The campus identifier.</param>
        /// <returns></returns>
        public static string StripSuffix( string textValue, int? campusId )
        {
            var fixedValue = string.Empty;
            if ( !string.IsNullOrWhiteSpace( textValue ) && campusId.HasValue )
            {
                var campus = CampusList.FirstOrDefault( c => c.Id == campusId );
                textValue = textValue.EndsWith( campus.Name, StringComparison.OrdinalIgnoreCase ) ? textValue.Substring( 0, textValue.IndexOf( campus.Name ) ) : textValue;
                textValue = textValue.EndsWith( campus.ShortCode, StringComparison.OrdinalIgnoreCase ) ? textValue.Substring( 0, textValue.IndexOf( campus.ShortCode ) ) : textValue;
            }

            // strip the suffix including delimiters
            fixedValue = textValue.IndexOfAny( ValidDelimiters ) >= 0
                ? textValue.Substring( 0, textValue.LastIndexOfAny( ValidDelimiters ) - 1 )
                : textValue;

            return CultureInfo.CurrentCulture.TextInfo.ToTitleCase( fixedValue.Trim() );
        }

        /// <summary>
        /// Adds a prayer request.
        /// </summary>
        /// <param name="rockContext">The RockContext object to work in for database access.</param>
        /// <param name="categoryName">The category of the prayer request.</param>
        /// <param name="requestText">The text of the prayer request.</param>
        /// <param name="requestDate">The request date of the prayer request.</param>
        /// <param name="foreignKey">The foreign key of the prayer request.</param>
        /// <param name="firstName">The first name for whom the request was submitted.</param>
        /// <param name="lastName">The last name for whom the request was submitted.</param>
        /// <param name="email">The email for whom the request was submitted.</param>
        /// <param name="expireDate">The date that the prayer request expires. Default: 14 days after request date.</param>
        /// <param name="allowComments">Flag to determine if the prayer request should allow comments. Default: <c>true</c>></param>
        /// <param name="isPublic">Flag to determine if the prayer request should be public. Default: <c>true</c></param>
        /// <param name="isApproved">Flag to determine if the prayer request is approved. Default: <c>true</c></param>
        /// <param name="approvedDate">Date the prayer request was approved. Default: <c>ImportDateTime</c></param>
        /// <param name="approvedById">Alias Id of who approved the prayer request. Default: <c>null</c></param>
        /// <param name="createdById">Alias Id of who entered the prayer request. Default: <c>null</c></param>
        /// <param name="requestedById">Alias Id of who submitted the prayer request. Default: <c>null</c></param>
        /// <param name="instantSave">Flag to determine if the prayer request should be saved to the rockContext prior to return. Default: <c>true</c></param>
        /// <returns>A newly created prayer request.</returns>
        public static PrayerRequest AddPrayerRequest( RockContext rockContext, string categoryName, string requestText, string requestDate, string foreignKey, string firstName,
            string lastName = "", string email = "", string expireDate = "", bool? allowComments = true, bool? isPublic = true, bool? isApproved = true, string approvedDate = "",
            int? approvedByAliasId = null, int? createdByAliasId = null, int? requestedByAliasId = null, string answerText = "", bool instantSave = true )
        {
            PrayerRequest prayerRequest = null;
            if ( !string.IsNullOrWhiteSpace( requestText ) )
            {
                rockContext = rockContext ?? new RockContext();

                if ( !string.IsNullOrWhiteSpace( foreignKey ) )
                {
                    prayerRequest = rockContext.PrayerRequests.AsQueryable().FirstOrDefault( p => p.ForeignKey.ToLower().Equals( foreignKey.ToLower() ) );
                }

                if ( prayerRequest == null )
                {
                    var prayerRequestDate = ( DateTime ) ParseDateOrDefault( requestDate, Bulldozer.BulldozerComponent.ImportDateTime );

                    prayerRequest = new PrayerRequest
                    {
                        FirstName = string.IsNullOrWhiteSpace( firstName ) ? "-" : firstName,
                        LastName = lastName,
                        Email = email,
                        Text = requestText,
                        EnteredDateTime = prayerRequestDate,
                        ExpirationDate = ParseDateOrDefault( expireDate, prayerRequestDate.AddDays( 14 ) ),
                        AllowComments = allowComments,
                        IsPublic = isPublic,
                        IsApproved = isApproved,
                        ApprovedOnDateTime = ( bool ) isApproved ? ParseDateOrDefault( approvedDate, Bulldozer.BulldozerComponent.ImportDateTime ) : null,
                        ApprovedByPersonAliasId = approvedByAliasId,
                        CreatedByPersonAliasId = createdByAliasId,
                        RequestedByPersonAliasId = requestedByAliasId,
                        ForeignKey = foreignKey,
                        ForeignId = foreignKey.AsType<int?>(),
                        Answer = answerText
                    };

                    if ( !string.IsNullOrWhiteSpace( categoryName ) )
                    {
                        //
                        // Try to find an existing category.
                        //
                        var category = rockContext.Categories.AsNoTracking().FirstOrDefault( c => c.EntityTypeId.Equals( prayerRequest.TypeId ) && c.Name.ToUpper().Equals( categoryName.ToUpper() ) );

                        //
                        // If not found, create one.
                        //
                        if ( category == null )
                        {
                            category = new Category
                            {
                                IsSystem = false,
                                EntityTypeId = prayerRequest.TypeId,
                                Name = categoryName,
                                Order = 0,
                                ParentCategoryId = AllChurchCategoryId
                            };

                            rockContext.Categories.Add( category );
                            rockContext.SaveChanges( DisableAuditing );
                        }

                        prayerRequest.CategoryId = category.Id;
                    }

                    if ( instantSave )
                    {
                        rockContext.PrayerRequests.Add( prayerRequest );
                        rockContext.SaveChanges( DisableAuditing );
                    }
                }
            }
            return prayerRequest;
        }

        /// <summary>
        /// Adds the connection type.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="typeName">Name of the type.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <returns></returns>
        public static ConnectionType AddConnectionType( RockContext rockContext, string typeName, bool instantSave = true )
        {
            var connectionType = new ConnectionType
            {
                Name = typeName,
                EnableFutureFollowup = true,
                EnableFullActivityList = true,
                RequiresPlacementGroupToConnect = false,
                IsActive = true
            };

            if ( instantSave )
            {
                rockContext.ConnectionTypes.Add( connectionType );
                rockContext.SaveChanges();
            }

            return connectionType;
        }

        /// <summary>
        /// Adds the connection status.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="statusName">Name of the status.</param>
        /// <param name="connectionTypeId">The connection type identifier.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <returns></returns>
        public static ConnectionStatus AddConnectionStatus( RockContext rockContext, string statusName, int connectionTypeId, bool instantSave = true )
        {
            var connectionStatus = new ConnectionStatus
            {
                Name = statusName,
                ConnectionTypeId = connectionTypeId
            };

            if ( instantSave )
            {
                rockContext.ConnectionStatuses.Add( connectionStatus );
                rockContext.SaveChanges();
            }

            return connectionStatus;
        }

        /// <summary>
        /// Adds the connection opportunity.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="defaultConnectionType">Default type of the connection.</param>
        /// <param name="createdDate">The created date.</param>
        /// <param name="opportunityName">The name.</param>
        /// <param name="foreignKey">The foreign key.</param>
        /// <param name="instantSave">if set to <c>true</c> [instant save].</param>
        /// <returns></returns>
        public static ConnectionOpportunity AddConnectionOpportunity( RockContext rockContext, int connectionTypeId, DateTime? createdDate, string opportunityName, string opportunityDescription, bool isActive, string foreignKey, bool instantSave = true )
        {
            var opportunity = new ConnectionOpportunity();
            opportunity.Name = opportunityName;
            opportunity.PublicName = opportunityName;
            opportunity.Description = opportunityDescription;
            opportunity.Summary = opportunityDescription;
            opportunity.ConnectionTypeId = connectionTypeId;
            opportunity.IsActive = isActive;
            opportunity.CreatedDateTime = createdDate;
            opportunity.ModifiedDateTime = createdDate;
            opportunity.ForeignKey = foreignKey;
            opportunity.ForeignId = foreignKey.AsIntegerOrNull();

            if ( instantSave )
            {
                rockContext.ConnectionOpportunities.Add( opportunity );
                rockContext.SaveChanges();
            }

            return opportunity;
        }

        /// <summary>
        /// Adds the connection request.
        /// </summary>
        /// <param name="statuses">The statuses.</param>
        /// <param name="opportunity">The opportunity.</param>
        /// <param name="rForeignKey">The r foreign key.</param>
        /// <param name="rCreatedDate">The r created date.</param>
        /// <param name="rModifiedDate">The r modified date.</param>
        /// <param name="rStatus">The r status.</param>
        /// <param name="rState">State of the r.</param>
        /// <param name="rComments">The r comments.</param>
        /// <param name="rFollowUp">The r follow up.</param>
        /// <param name="requestor">The requester.</param>
        /// <param name="requestConnector">The request connector.</param>
        /// <returns></returns>
        public static ConnectionRequest AddConnectionRequest( ConnectionOpportunity opportunity, string rForeignKey, DateTime? rCreatedDate, DateTime? rModifiedDate, int rStatusId, ConnectionState rState, string rComments, DateTime? rFollowUp, int requestorAliasId, int? connectorAliasId )
        {
            ConnectionRequest request = new ConnectionRequest();
            request.ConnectionOpportunityId = opportunity.Id;
            request.PersonAliasId = requestorAliasId;
            request.Comments = rComments;
            request.ConnectionStatusId = rStatusId;
            request.ConnectionState = rState;
            request.ConnectorPersonAliasId = connectorAliasId;
            request.FollowupDate = rFollowUp;
            request.CreatedDateTime = rCreatedDate;
            request.ModifiedDateTime = rModifiedDate;
            request.ForeignKey = rForeignKey;
            request.ForeignId = rForeignKey.AsIntegerOrNull();
            request.ConnectionRequestActivities = new List<ConnectionRequestActivity>();
            return request;
        }

        /// <summary>
        /// Adds the connection activity.
        /// </summary>
        /// <param name="opportunity">The opportunity.</param>
        /// <param name="rForeignKey">The r foreign key.</param>
        /// <param name="aNote">a note.</param>
        /// <param name="aCreatedDate">a created date.</param>
        /// <param name="activityConnector">The activity connector.</param>
        /// <param name="activityType">Type of the activity.</param>
        /// <returns></returns>
        public static ConnectionRequestActivity AddConnectionActivity( int opportunityId, string aNote, DateTime? aCreatedDate, int? connectorAliasId, int activityTypeId, string rForeignKey )
        {
            var activity = new ConnectionRequestActivity();
            activity.ConnectionActivityTypeId = activityTypeId;
            activity.ConnectorPersonAliasId = connectorAliasId;
            activity.ConnectionOpportunityId = opportunityId;
            activity.Note = aNote;
            activity.CreatedDateTime = aCreatedDate;
            activity.ModifiedDateTime = aCreatedDate;
            activity.ForeignKey = rForeignKey;
            activity.ForeignId = rForeignKey.AsIntegerOrNull();
            return activity;
        }

        /// <summary>
        /// Adds a person previous last name.
        /// </summary>
        /// <param name="rockContext">The RockContext object to work in for database access.</param>
        /// <param name="personPreviousName">The person previous last name to be added.</param>
        /// <param name="personAliasId">The person alias for the person previous name.</param>
        /// <param name="fk">The foreign key for the person previous name.</param>
        /// <param name="instantSave">Flag to determine if the prayer request should be saved to the rockContext prior to return. Default: <c>true</c></param>
        /// <returns>A newly created prayer request.</returns>
        public static PersonPreviousName AddPersonPreviousName( RockContext rockContext, string personPreviousName, int personAliasId, string fk = "", bool instantSave = true )
        {
            PersonPreviousName previousName = null;
            if ( !string.IsNullOrWhiteSpace( personPreviousName ) )
            {
                rockContext = rockContext ?? new RockContext();

                previousName = new PersonPreviousName
                {
                    LastName = personPreviousName,
                    PersonAliasId = personAliasId,
                    ForeignKey = fk,
                    ForeignGuid = fk.AsGuidOrNull(),
                    ForeignId = fk.AsIntegerOrNull()
                };

                if ( instantSave )
                {
                    rockContext.PersonPreviousNames.Add( previousName );
                    rockContext.SaveChanges( DisableAuditing );
                }
            }
            return previousName;
        }

        /// <summary>
        /// Add a new financial gateway to the Rock system using the Test Gateway entity.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="gatewayName">The name of the gateway to be added to Rock.</param>
        /// <returns>A newly created financial gateway.</returns>
        public static FinancialGateway AddFinancialGateway( RockContext rockContext, string gatewayName, bool instantSave = true )
        {
            var gateway = new FinancialGateway();
            gateway.Name = string.IsNullOrWhiteSpace( gatewayName ) ? "Imported Gateway" : gatewayName;
            gateway.EntityTypeId = TestGatewayTypeId;
            gateway.BatchTimeOffsetTicks = 0;
            gateway.IsActive = true;

            if ( instantSave )
            {
                var gatewayService = new Rock.Model.FinancialGatewayService( rockContext );
                gatewayService.Add( gateway );

                rockContext.SaveChanges();
            }
            return gateway;
        }

        /// <summary>
        /// Gets a location based on address fields or creates a new one.
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="address">The street address.</param>
        /// <param name="address2">The secondary street address.</param>
        /// <param name="city">The city.</param>
        /// <param name="state">The state.</param>
        /// <param name="postalCode">The postal code.</param>
        /// <param name="country">The country.</param>
        /// <returns>A Rock Location.</returns>
        public static Location GetOrAddLocation( RockContext rockContext, string address, string address2, string city, string state, string postalCode, string country )
        {
            // Default country to US if country is not provided but at least one other address field is provided
            if ( string.IsNullOrWhiteSpace( country ) && ( !string.IsNullOrWhiteSpace( address ) || !string.IsNullOrWhiteSpace( address2 ) || !string.IsNullOrWhiteSpace( city ) || !string.IsNullOrWhiteSpace( state ) || !string.IsNullOrWhiteSpace( postalCode ) ) )
            {
                country = "US";
            }

            Location locAddress = new LocationService( rockContext ).Get( address.Left( 100 ), address2.Left( 100 ), city, state, postalCode, country, verifyLocation: false );

            return locAddress;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="rockContext">The rock context.</param>
        /// <param name="entityType">The entityType of the entity the note is for.</param>
        /// <param name="entityId">The entityId of the entity the note is for.</param>
        /// <param name="categoryName">The category of the note.</param>
        /// <param name="verb">The verb of the note.</param>
        /// <param name="changeType">The change type of the note.</param>
        /// <param name="caption">The caption of the note.</param>
        /// <param name="valueName">The valuename of the note.</param>
        /// <param name="newValue">The new value.</param>
        /// <param name="oldValue">The old value.</param>
        /// <param name="relatedEntityTypeId">The entity type id of the related entity.</param>
        /// <param name="relatedEntityId">The entity id of the related entity.</param>
        /// <param name="isSensitive">If set to <c>true</c> [is private].</param>
        /// <param name="isSystem">If set to <c>true</c> [is system].</param>
        /// <param name="dateCreated">The date created.</param>
        /// <param name="foreignKey">The foreignKey.</param>
        /// <param name="creatorPersonAliasId">The person alias id of the creator of the note.</param>
        /// <param name="instantSave">If set to <c>true</c> [instant save].</param>
        /// <returns></returns>
        public static History AddHistory( RockContext rockContext, EntityTypeCache entityType, int entityId, string categoryName, string verb = null, string changeType = null, 
            string caption = null, string valueName = null, string newValue = null, string oldValue = null, int? relatedEntityTypeId = null, 
            int? relatedEntityId = null, bool isSensitive = false, bool isSystem = false, DateTime? dateCreated = null, string foreignKey = null, 
            int? creatorPersonAliasId = null, bool instantSave = true )
        {
            // ensure we have enough information to create a history object
            if ( entityType == null || entityId <= 0 || string.IsNullOrWhiteSpace( categoryName ) )
            {
                return null;
            }

            rockContext = rockContext ?? new RockContext();
            var historyEntityType = EntityTypeCache.Get( "546D5F43-1184-47C9-8265-2D7BF4E1BCA5".AsGuid() );
            int parentCategoryId = -1;

            var entityTypeInstance = entityType.GetEntityType();
            var parentCategory = "Person";  // default parent category to Person

            if ( entityTypeInstance == typeof( Group ) || entityTypeInstance == typeof( GroupMember ) )
            {
                parentCategory = "Group";
            }

            switch ( parentCategory )         
            {
                case "Person":
                    {
                        parentCategoryId = CategoryCache.Get( Rock.SystemGuid.Category.HISTORY_PERSON ).Id;
                        break;
                    }
                case "Financial":
                    {
                        parentCategoryId = CategoryCache.Get( "E41FC407-B60E-4B85-954D-D27F0762114B".AsGuid() ).Id;  // Financial parent category
                        break;
                    }
                case "Event":
                    {
                        parentCategoryId = CategoryCache.Get( "035CDEDA-7BB9-4E42-B7FD-E0B7487108E5".AsGuid() ).Id;  // Event parent category
                        break;
                    }
                case "Group":
                    {
                        parentCategoryId = CategoryCache.Get( Rock.SystemGuid.Category.HISTORY_GROUP ).Id;
                        break;
                    }
                case "Connection Request":
                    {
                        parentCategoryId = CategoryCache.Get( Rock.SystemGuid.Category.HISTORY_CONNECTION_REQUEST ).Id;
                        break;
                    }
                default:
                    {
                        break;
                    }
            }

            var category = GetCategory( rockContext, historyEntityType.Id, parentCategoryId, categoryName, findOnly: false );

            // create the history entry on this person
            var history = new History
            {
                EntityTypeId = entityType.Id,
                EntityId = entityId,
                CategoryId = category.Id,
                IsSystem = isSystem,
                IsSensitive = isSensitive,
                Verb = verb,
                ChangeType = changeType,
                Caption = caption,
                ValueName = valueName,
                NewValue = newValue,
                OldValue = oldValue,
                RelatedEntityTypeId = relatedEntityTypeId,
                RelatedEntityId = relatedEntityId,
                ForeignId = foreignKey.AsIntegerOrNull(),
                ForeignKey = foreignKey,
                CreatedDateTime = dateCreated,
                CreatedByPersonAliasId = creatorPersonAliasId
            };

            if ( instantSave )
            {
                rockContext.Histories.Add( history );
                rockContext.SaveChanges( DisableAuditing );
            }

            return history;
        }
    }
}