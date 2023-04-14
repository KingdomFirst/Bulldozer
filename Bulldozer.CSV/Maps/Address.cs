using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the address import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Import person addresses.
        /// </summary>
        /// <returns></returns>
        public int ImportPersonAddressList()
        {
            var unmatchedAddresses = PersonAddressCsvList.Where( pa => !PersonCsvList.Select( p => p.Id ).Any( i => i == pa.PersonId ) );
            var additionalAddresses = unmatchedAddresses.Where( pa => PersonDict.ContainsKey( ImportInstanceFKPrefix + "_" + pa.PersonId ) );
            if ( additionalAddresses.Any() )
            {
                // get the distinct addresses for each family
                var distinctAddresses = additionalAddresses.GroupBy( a => new
                {
                    FamilyId = PersonDict[ImportInstanceFKPrefix + "_" + a.PersonId].PrimaryFamilyId,
                    a.AddressType,
                    a.Street1,
                    a.Street2,
                    a.City,
                    a.State
                } )
                .Select( ga => new GroupAddressImport
                {
                    FamilyId = ga.Key.FamilyId,
                    GroupLocationTypeValueId = GetGroupLocationTypeDVId( ga.Key.AddressType ).GetValueOrDefault(),
                    City = ga.Key.City,
                    Street1 = ga.Key.Street1,
                    Street2 = ga.Key.Street2,
                    State = ga.Key.State,
                    Latitude = ga.Max( a => a.Latitude.AsDoubleOrNull() ),
                    Longitude = ga.Max( a => a.Longitude.AsDoubleOrNull() ),
                    Country = ga.Max( a => a.Country ),
                    IsMailingLocation = ga.Max( a => a.IsMailing ),
                    IsMappedLocation = ga.Key.AddressType == AddressType.Home,
                    PostalCode = ga.Max( a => a.PostalCode ),
                    AddressForeignKey = ga.Max( a => a.AddressId )
                } )
                .ToList();

                ReportProgress( 0, string.Format( "Begin processing {0} address records tied to previously imported people...", distinctAddresses.Count ) );
                return ImportPersonAddressList( distinctAddresses );
            }
            return 0;
        }
        /// <summary>
        /// Import person addresses.
        /// </summary>
        /// <param name="addressImports">The person address imports.</param>
        /// <returns></returns>
        public int ImportPersonAddressList( List<GroupAddressImport> addressImports )
        {
            // Slice data into chunks and process
            var addressesRemainingToProcess = addressImports.Count;
            var addressesCompleted = 0;

            while ( addressesRemainingToProcess > 0 )
            {
                if ( addressesCompleted > 0 && addressesCompleted % ( this.PersonChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} Person Address records processed.", addressesCompleted ) );
                }

                if ( addressesCompleted % ( this.PersonChunkSize ) < 1 )
                {
                    var addressChunk = addressImports.Take( Math.Min( this.PersonChunkSize, addressImports.Count ) ).ToList();
                    addressesCompleted += BulkAddressImport( addressChunk );
                    addressesRemainingToProcess -= addressChunk.Count;
                    addressImports.RemoveRange( 0, addressChunk.Count );
                    ReportPartialProgress();
                }
            }
            return addressesCompleted;
        }

        /// <summary>
        /// Bulk import of Addresses.
        /// </summary>
        /// <param name="addressImports">The person address imports.</param>
        /// <returns></returns>
        public int BulkAddressImport( List<GroupAddressImport> addressImports )
        {
            var rockContext = new RockContext();
            var locationCreatedDateTimeStart = RockDateTime.Now;
            var groupLocationsToInsert = new List<GroupLocation>();
            var locationsToInsert = new List<Location>();
            var locationService = new LocationService( rockContext );
            foreach ( var address in addressImports )
            {
                Location location = new Location
                {
                    Street1 = address.Street1.Left( 100 ),
                    Street2 = address.Street2.Left( 100 ),
                    City = address.City.Left( 50 ),
                    State = address.State.Left( 50 ),
                    Country = address.Country.Left( 50 ),
                    PostalCode = address.PostalCode.Left( 50 ),
                    CreatedDateTime = locationCreatedDateTimeStart,
                    ModifiedDateTime = locationCreatedDateTimeStart,
                };

                if ( address.Latitude.HasValue && address.Longitude.HasValue )
                {
                    location.SetLocationPointFromLatLong( address.Latitude.Value, address.Longitude.Value );
                }

                var newGroupLocation = new GroupLocation
                {
                    GroupLocationTypeValueId = address.GroupLocationTypeValueId,
                    GroupId = address.FamilyId.Value,
                    IsMailingLocation = address.IsMailingLocation,
                    IsMappedLocation = address.IsMappedLocation,
                    CreatedDateTime = locationCreatedDateTimeStart,
                    ModifiedDateTime = locationCreatedDateTimeStart,
                    Location = location,
                    ForeignKey = this.ImportInstanceFKPrefix + "_" + address.AddressForeignKey,
                    ForeignId = address.AddressForeignKey.AsInteger()
                };

                groupLocationsToInsert.Add( newGroupLocation );
                locationsToInsert.Add( newGroupLocation.Location );
            }
            rockContext.BulkInsert( locationsToInsert );

            var locationIdLookup = locationService.Queryable().Select( a => new { a.Id, a.Guid } ).ToList().ToDictionary( k => k.Guid, v => v.Id );
            foreach ( var groupLocation in groupLocationsToInsert )
            {
                groupLocation.LocationId = locationIdLookup[groupLocation.Location.Guid];
            }

            rockContext.BulkInsert( groupLocationsToInsert );
            return addressImports.Count;
        }
    }
}
