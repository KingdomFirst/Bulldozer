﻿// <copyright>
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
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using OrcaMDF.Core.MetaData;
using Rock.Data;
using Rock.Model;

namespace Bulldozer.Utility
{
    /// <summary>
    /// Extensions to the base components
    /// </summary>
    public static partial class Extensions
    {
        /// <summary>
        /// Gets the C# type from a SQL or OrcaMDF type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentOutOfRangeException">Conversion failed: type not recognized  + type</exception>
        public static Type GetSQLType( ColumnType type )
        {
            switch ( type )
            {
                case ColumnType.BigInt:
                    return typeof( long );

                case ColumnType.Binary:
                case ColumnType.Image:
                case ColumnType.VarBinary:
                    return typeof( byte[] );

                case ColumnType.Bit:
                    return typeof( Boolean );

                case ColumnType.Char:
                case ColumnType.NChar:
                case ColumnType.NText:
                case ColumnType.NVarchar:
                case ColumnType.RID:
                case ColumnType.Text:
                case ColumnType.Varchar:
                    return typeof( string );

                case ColumnType.DateTime:
                case ColumnType.SmallDatetime:
                    return typeof( DateTime );

                case ColumnType.Decimal:
                case ColumnType.Money:
                case ColumnType.SmallMoney:
                    return typeof( decimal );

                case ColumnType.Int:
                    return typeof( int );

                case ColumnType.Uniquifier:
                case ColumnType.UniqueIdentifier:
                    return typeof( Guid );

                case ColumnType.SmallInt:
                    return typeof( short );

                case ColumnType.TinyInt:
                    return typeof( byte );

                case ColumnType.Variant:
                    return typeof( object );

                default:
                    throw new ArgumentOutOfRangeException( "Conversion failed: type not recognized " + type );
            }
        }

        /// <summary>
        /// Gets the enumerable values.
        /// http://damieng.com/blog/2008/04/10/using-linq-to-foreach-over-an-enum-in-c
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static IEnumerable<T> Get<T>()
        {
            return Enum.GetValues( typeof( T ) ).Cast<T>();
        }

        /// <summary>
        /// Gets the MIME type of the file.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <returns></returns>
        public static string GetMIMEType( string fileName )
        {
            //get file extension
            var extension = Path.GetExtension( fileName ).ToLowerInvariant();

            if ( extension.Length > 0 &&
                MIMETypesDictionary.ContainsKey( extension.Remove( 0, 1 ) ) )
            {
                return MIMETypesDictionary[extension.Remove( 0, 1 )];
            }
            return "application/octet-stream";
        }

        /// <summary>
        /// Strips the whitespace.
        /// </summary>
        /// <param name="str">The string.</param>
        /// <returns></returns>
        public static string RemoveWhitespace( this string str )
        {
            var stringBuilder = new StringBuilder();
            foreach ( char c in str )
            {
                if ( !char.IsWhiteSpace( c ) )
                {
                    stringBuilder.Append( c );
                }
            }
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns true if the given string is a valid email address.
        /// </summary>
        /// <param name="email">The string to validate</param>
        /// <param name="pattern">The regex pattern to use to validate the email</param>
        /// <returns>true if valid email, false otherwise</returns>
        public static bool IsEmail( this string email, string pattern = null )
        {
            if ( string.IsNullOrWhiteSpace( pattern ) )
            {
                pattern = @"\s*(?:[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*|""(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*"")@(?:(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[A-Za-z0-9-]*[A-Za-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])\s*";
            }
            return Regex.IsMatch( email, pattern );
        }

        /// <summary>
        /// Parse the string as a date time value. If the parse was unsuccessful then return
        /// the defaultValue.
        /// </summary>
        /// <param name="stringValue">The string value.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns></returns>
        public static DateTime? ParseDateOrDefault( string stringValue, DateTime? defaultValue )
        {
            DateTime parsed;
            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy",
                                      "MM-dd-yyyy", "M-d-yyyy", "M-dd-yyyy", "MM-dd-yy",
                                      "M/d/yyyy", "M/dd/yyyy",
                                      "M/d/yyyy h:mm:ss tt", "M/d/yyyy h:mm tt",
                                      "MM/dd/yyyy hh:mm:ss", "M/d/yyyy h:mm:ss",
                                      "M/d/yyyy hh:mm tt", "M/d/yyyy hh tt",
                                      "M/d/yyyy h:mm", "M/d/yyyy h:mm",
                                      "MM/dd/yyyy hh:mm", "M/dd/yyyy hh:mm", "yyyy-MM-dd HH:mm:ss",
                                      "yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss.ffff", "yyyy-MM-dd HH:mm:ss.fffff",
                                      "yyyy-MM-dd HH:mm:ss.ffffff", "yyyy-MM-dd HH:mm:ss.fffffff", "yyyy-MM-dd HH:mm:ss.fff tt" };

            if ( DateTime.TryParseExact( stringValue, dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out parsed ) )
            {
                return parsed;
            }
            else
            {
                return defaultValue;
            }
        }

        /// <summary>
        /// Parse the string as a boolean value. If the parse was unsuccessful then return
        /// the defaultValue instead. Valid values are: TRUE, T, YES, Y, FALSE, F, NO, N, or any numeric value.
        /// </summary>
        /// <param name="stringValue">The string value.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns></returns>
        public static bool? ParseBoolOrDefault( string stringValue, bool? defaultValue )
        {
            bool parsed;

            if ( TryParseBool( stringValue, out parsed ) )
            {
                return parsed;
            }
            else
            {
                return defaultValue;
            }
        }

        /// <summary>
        /// Legal values: Case insensitive strings TRUE/FALSE, T/F, YES/NO, Y/N, numbers (0 =&gt; false, non-zero =&gt; true)
        /// Similar to "bool.TryParse(string text, out bool)" except that it handles values other than 'true'/'false' and handles numbers like C/C++
        /// Taken from: http://stackoverflow.com/questions/9191924/why-bool-try-parse-not-parsing-value-to-true-or-false
        /// </summary>
        /// <param name="inVal">The in value.</param>
        /// <param name="retVal">if set to <c>true</c> [ret value].</param>
        /// <returns></returns>
        public static bool TryParseBool( object inVal, out bool retVal )
        {
            // There are a couple of built-in ways to convert values to boolean, but unfortunately they skip things like YES/NO, 1/0, T/F
            //bool.TryParse(string, out bool retVal) (.NET 4.0 Only); Convert.ToBoolean(object) (requires try/catch)
            inVal = ( inVal ?? "" ).ToString().Trim().ToUpper();
            switch ( ( string ) inVal )
            {
                case "TRUE":
                case "T":
                case "YES":
                case "Y":
                    retVal = true;
                    return true;

                case "FALSE":
                case "F":
                case "NO":
                case "N":
                    retVal = false;
                    return true;

                default:
                    // If value can be parsed as a number, 0==false, non-zero==true (old C/C++ usage)
                    double number;
                    if ( double.TryParse( ( string ) inVal, out number ) )
                    {
                        retVal = ( number != 0 );
                        return true;
                    }
                    // If not a valid value for conversion, return false (not parsed)
                    retVal = false;
                    return false;
            }
        }

        /// <summary>
        /// Creates the content of the iCalendar event.
        /// </summary>
        /// <param name="dateTime">The date time.</param>
        /// <param name="importDateTime">The import date time.</param>
        /// <returns></returns>
        public static string CreateCalendarContent( DateTime startDateTime, string frequency, DateTime? importDateTime = null )
        {
            importDateTime = importDateTime ?? Rock.RockDateTime.Now;

            var builder = new StringBuilder();
            builder.Append( "BEGIN:VCALENDAR" ).Append( Environment.NewLine );
            builder.Append( "VERSION:2.0" ).Append( Environment.NewLine );
            builder.Append( "BEGIN:VEVENT" ).Append( Environment.NewLine );
            builder.Append( "UID:" ).Append( Guid.NewGuid().ToString() ).Append( Environment.NewLine );
            builder.Append( string.Format( "RRULE:FREQ={0};BYDAY={1}", frequency, startDateTime.ToString( "ddd" ) ) ).Append( Environment.NewLine );
            builder.Append( "DTSTART:" ).Append( startDateTime.ToString( "yyyyMMddTHHmmssZ" ) ).Append( Environment.NewLine );
            builder.Append( "DTSTAMP:" ).Append( ( ( DateTime ) importDateTime ).ToString( "yyyyMMddTHHmmssZ" ) ).Append( Environment.NewLine );
            builder.Append( "END:VEVENT" ).Append( Environment.NewLine );
            builder.Append( "END:VCALENDAR" ).Append( Environment.NewLine );

            return builder.ToString();
        }

        /// <summary>
        /// Detach all entity objects from the change tracker that is associated with
        /// the given RockContext. Any entity that is stored (for example in an Imported...
        /// variable) should be detached before the function ends. If it is explicitely
        /// attached and then not detached then when it is attached to another context
        /// an exception "An entity object cannot be referenced by multiple instances
        /// of IEntityChangeTracker" occurs.
        /// Taken from: http://stackoverflow.com/questions/2465933/how-to-clean-up-an-entity-framework-object-context
        /// </summary>
        /// <param name="context">The context.</param>
        public static void DetachAllInContext( RockContext context )
        {
            foreach ( var dbEntityEntry in context.ChangeTracker.Entries() )
            {
                if ( dbEntityEntry.Entity != null )
                {
                    dbEntityEntry.State = EntityState.Detached;
                }
            }
        }

        /// <summary>
        /// Helper method to determine the MIME type of a certain file
        /// Reused from http://stackoverflow.com/a/7161265
        /// </summary>
        private static readonly Dictionary<string, string> MIMETypesDictionary = new Dictionary<string, string>
        {
            {"ai", "application/postscript"},
            {"aif", "audio/x-aiff"},
            {"aifc", "audio/x-aiff"},
            {"aiff", "audio/x-aiff"},
            {"asc", "text/plain"},
            {"atom", "application/atom+xml"},
            {"au", "audio/basic"},
            {"avi", "video/x-msvideo"},
            {"bcpio", "application/x-bcpio"},
            {"bin", "application/octet-stream"},
            {"bmp", "image/bmp"},
            {"cdf", "application/x-netcdf"},
            {"cgm", "image/cgm"},
            {"class", "application/octet-stream"},
            {"cpio", "application/x-cpio"},
            {"cpt", "application/mac-compactpro"},
            {"csh", "application/x-csh"},
            {"css", "text/css"},
            {"dcr", "application/x-director"},
            {"dif", "video/x-dv"},
            {"dir", "application/x-director"},
            {"djv", "image/vnd.djvu"},
            {"djvu", "image/vnd.djvu"},
            {"dll", "application/octet-stream"},
            {"dmg", "application/octet-stream"},
            {"dms", "application/octet-stream"},
            {"doc", "application/msword"},
            {"docx","application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
            {"dotx", "application/vnd.openxmlformats-officedocument.wordprocessingml.template"},
            {"docm","application/vnd.ms-word.document.macroEnabled.12"},
            {"dotm","application/vnd.ms-word.template.macroEnabled.12"},
            {"dtd", "application/xml-dtd"},
            {"dv", "video/x-dv"},
            {"dvi", "application/x-dvi"},
            {"dxr", "application/x-director"},
            {"eps", "application/postscript"},
            {"etx", "text/x-setext"},
            {"exe", "application/octet-stream"},
            {"ez", "application/andrew-inset"},
            {"gif", "image/gif"},
            {"gram", "application/srgs"},
            {"grxml", "application/srgs+xml"},
            {"gtar", "application/x-gtar"},
            {"hdf", "application/x-hdf"},
            {"hqx", "application/mac-binhex40"},
            {"htm", "text/html"},
            {"html", "text/html"},
            {"ice", "x-conference/x-cooltalk"},
            {"ico", "image/x-icon"},
            {"ics", "text/calendar"},
            {"ief", "image/ief"},
            {"ifb", "text/calendar"},
            {"iges", "model/iges"},
            {"igs", "model/iges"},
            {"jnlp", "application/x-java-jnlp-file"},
            {"jp2", "image/jp2"},
            {"jpe", "image/jpeg"},
            {"jpeg", "image/jpeg"},
            {"jpg", "image/jpeg"},
            {"js", "application/x-javascript"},
            {"kar", "audio/midi"},
            {"latex", "application/x-latex"},
            {"lha", "application/octet-stream"},
            {"lzh", "application/octet-stream"},
            {"m3u", "audio/x-mpegurl"},
            {"m4a", "audio/mp4a-latm"},
            {"m4b", "audio/mp4a-latm"},
            {"m4p", "audio/mp4a-latm"},
            {"m4u", "video/vnd.mpegurl"},
            {"m4v", "video/x-m4v"},
            {"mac", "image/x-macpaint"},
            {"man", "application/x-troff-man"},
            {"mathml", "application/mathml+xml"},
            {"me", "application/x-troff-me"},
            {"mesh", "model/mesh"},
            {"mid", "audio/midi"},
            {"midi", "audio/midi"},
            {"mif", "application/vnd.mif"},
            {"mov", "video/quicktime"},
            {"movie", "video/x-sgi-movie"},
            {"mp2", "audio/mpeg"},
            {"mp3", "audio/mpeg"},
            {"mp4", "video/mp4"},
            {"mpe", "video/mpeg"},
            {"mpeg", "video/mpeg"},
            {"mpg", "video/mpeg"},
            {"mpga", "audio/mpeg"},
            {"ms", "application/x-troff-ms"},
            {"msh", "model/mesh"},
            {"mxu", "video/vnd.mpegurl"},
            {"nc", "application/x-netcdf"},
            {"oda", "application/oda"},
            {"ogg", "application/ogg"},
            {"pbm", "image/x-portable-bitmap"},
            {"pct", "image/pict"},
            {"pdb", "chemical/x-pdb"},
            {"pdf", "application/pdf"},
            {"pgm", "image/x-portable-graymap"},
            {"pgn", "application/x-chess-pgn"},
            {"pic", "image/pict"},
            {"pict", "image/pict"},
            {"png", "image/png"},
            {"pnm", "image/x-portable-anymap"},
            {"pnt", "image/x-macpaint"},
            {"pntg", "image/x-macpaint"},
            {"ppm", "image/x-portable-pixmap"},
            {"ppt", "application/vnd.ms-powerpoint"},
            {"pptx","application/vnd.openxmlformats-officedocument.presentationml.presentation"},
            {"potx","application/vnd.openxmlformats-officedocument.presentationml.template"},
            {"ppsx","application/vnd.openxmlformats-officedocument.presentationml.slideshow"},
            {"ppam","application/vnd.ms-powerpoint.addin.macroEnabled.12"},
            {"pptm","application/vnd.ms-powerpoint.presentation.macroEnabled.12"},
            {"potm","application/vnd.ms-powerpoint.template.macroEnabled.12"},
            {"ppsm","application/vnd.ms-powerpoint.slideshow.macroEnabled.12"},
            {"ps", "application/postscript"},
            {"qt", "video/quicktime"},
            {"qti", "image/x-quicktime"},
            {"qtif", "image/x-quicktime"},
            {"ra", "audio/x-pn-realaudio"},
            {"ram", "audio/x-pn-realaudio"},
            {"ras", "image/x-cmu-raster"},
            {"rdf", "application/rdf+xml"},
            {"rgb", "image/x-rgb"},
            {"rm", "application/vnd.rn-realmedia"},
            {"roff", "application/x-troff"},
            {"rtf", "text/rtf"},
            {"rtx", "text/richtext"},
            {"sgm", "text/sgml"},
            {"sgml", "text/sgml"},
            {"sh", "application/x-sh"},
            {"shar", "application/x-shar"},
            {"silo", "model/mesh"},
            {"sit", "application/x-stuffit"},
            {"skd", "application/x-koan"},
            {"skm", "application/x-koan"},
            {"skp", "application/x-koan"},
            {"skt", "application/x-koan"},
            {"smi", "application/smil"},
            {"smil", "application/smil"},
            {"snd", "audio/basic"},
            {"so", "application/octet-stream"},
            {"spl", "application/x-futuresplash"},
            {"src", "application/x-wais-source"},
            {"sv4cpio", "application/x-sv4cpio"},
            {"sv4crc", "application/x-sv4crc"},
            {"svg", "image/svg+xml"},
            {"swf", "application/x-shockwave-flash"},
            {"t", "application/x-troff"},
            {"tar", "application/x-tar"},
            {"tcl", "application/x-tcl"},
            {"tex", "application/x-tex"},
            {"texi", "application/x-texinfo"},
            {"texinfo", "application/x-texinfo"},
            {"tif", "image/tiff"},
            {"tiff", "image/tiff"},
            {"tr", "application/x-troff"},
            {"tsv", "text/tab-separated-values"},
            {"txt", "text/plain"},
            {"ustar", "application/x-ustar"},
            {"vcd", "application/x-cdlink"},
            {"vrml", "model/vrml"},
            {"vxml", "application/voicexml+xml"},
            {"wav", "audio/x-wav"},
            {"wbmp", "image/vnd.wap.wbmp"},
            {"wbmxl", "application/vnd.wap.wbxml"},
            {"wml", "text/vnd.wap.wml"},
            {"wmlc", "application/vnd.wap.wmlc"},
            {"wmls", "text/vnd.wap.wmlscript"},
            {"wmlsc", "application/vnd.wap.wmlscriptc"},
            {"wrl", "model/vrml"},
            {"xbm", "image/x-xbitmap"},
            {"xht", "application/xhtml+xml"},
            {"xhtml", "application/xhtml+xml"},
            {"xls", "application/vnd.ms-excel"},
            {"xml", "application/xml"},
            {"xpm", "image/x-xpixmap"},
            {"xsl", "application/xml"},
            {"xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            {"xltx","application/vnd.openxmlformats-officedocument.spreadsheetml.template"},
            {"xlsm","application/vnd.ms-excel.sheet.macroEnabled.12"},
            {"xltm","application/vnd.ms-excel.template.macroEnabled.12"},
            {"xlam","application/vnd.ms-excel.addin.macroEnabled.12"},
            {"xlsb","application/vnd.ms-excel.sheet.binary.macroEnabled.12"},
            {"xslt", "application/xslt+xml"},
            {"xul", "application/vnd.mozilla.xul+xml"},
            {"xwd", "image/x-xwindowdump"},
            {"xyz", "chemical/x-xyz"},
            {"zip", "application/zip"}
        };

        /// <summary>
        /// Gets the entity type qualifier value by searching the foreign key.
        /// </summary>
        /// <param name="entityTypeQualifierColumn">The entity type qualifier column.</param>
        /// <param name="entityTypeQualifierValue">The entity type qualifier value.</param>
        /// <param name="rockContext">The rock context.</param>
        /// <returns></returns>
        public static string GetEntityTypeQualifierValue( string entityTypeQualifierColumn, string entityTypeQualifierValue, RockContext rockContext = null )
        {
            rockContext = rockContext ?? new RockContext();

            var id = entityTypeQualifierValue;

            switch ( entityTypeQualifierColumn )
            {
                case "ConnectionTypeId":
                    var connectionType = new ConnectionTypeService( rockContext )
                        .Queryable()
                        .AsNoTracking()
                        .FirstOrDefault( v => v.ForeignKey.Equals( entityTypeQualifierValue, StringComparison.OrdinalIgnoreCase ) );
                    if ( connectionType != null )
                    {
                        id = connectionType.Id.ToString();
                    }
                    break;

                case "ContentChannelId":
                    var contentChannel = new ContentChannelService( rockContext )
                        .Queryable()
                        .AsNoTracking()
                        .FirstOrDefault( v => v.ForeignKey.Equals( entityTypeQualifierValue, StringComparison.OrdinalIgnoreCase ) );
                    if ( contentChannel != null )
                    {
                        id = contentChannel.Id.ToString();
                    }
                    break;

                case "ContentChannelTypeId":
                    var contentChannelType = new ContentChannelTypeService( rockContext )
                        .Queryable()
                        .AsNoTracking()
                        .FirstOrDefault( v => v.ForeignKey.Equals( entityTypeQualifierValue, StringComparison.OrdinalIgnoreCase ) );
                    if ( contentChannelType != null )
                    {
                        id = contentChannelType.Id.ToString();
                    }
                    break;

                case "DefinedTypeId":
                    var definedType = new DefinedTypeService( rockContext )
                        .Queryable()
                        .AsNoTracking()
                        .FirstOrDefault( v => v.ForeignKey.Equals( entityTypeQualifierValue, StringComparison.OrdinalIgnoreCase ) );
                    if ( definedType != null )
                    {
                        id = definedType.Id.ToString();
                    }
                    break;

                case "GroupId":
                    var group = new GroupService( rockContext )
                        .Queryable()
                        .AsNoTracking()
                        .FirstOrDefault( v => v.ForeignKey.Equals( entityTypeQualifierValue, StringComparison.OrdinalIgnoreCase ) );
                    if ( group != null )
                    {
                        id = group.Id.ToString();
                    }
                    break;

                case "GroupTypeId":
                    var groupType = new GroupTypeService( rockContext )
                        .Queryable()
                        .AsNoTracking()
                        .FirstOrDefault( v => v.ForeignKey.Equals( entityTypeQualifierValue, StringComparison.OrdinalIgnoreCase ) );
                    if ( groupType != null )
                    {
                        id = groupType.Id.ToString();
                    }
                    break;
            }

            return id;
        }
        /// <summary>
        /// The Minimum Date Boundary for a DateTime field in SQL Server.
        /// </summary>
        public static readonly DateTime MinSqlDate = new DateTime( 1753, 1, 1 );

        /// <summary>
        /// The Maximum Date Boundary for a DateTime field in SQL Server.
        /// </summary>
        public static readonly DateTime MaxSqlDate = new DateTime( 9999, 12, 31, 23, 59, 59, 99 );


        /// <summary>
        /// Gets a date value that is within the safe range of SQL Server DateTime ranges (see MinSqlDate and MaxSqlDate).
        /// </summary>
        /// <param name="dateTime">The <see cref="DateTime"/>.</param>
        /// <returns></returns>
        public static DateTime ToSQLSafeDate( this DateTime dateTime )
        {
            if ( dateTime <= MinSqlDate )
            {
                return MinSqlDate;
            }

            if ( dateTime >= MaxSqlDate )
            {
                return MaxSqlDate;
            }

            return dateTime;
        }

        /// <summary>
        /// Gets a nullable date value that is within the safe range of SQL Server DateTime ranges (see MinSqlDate and MaxSqlDate).
        /// </summary>
        /// <param name="dateTime">The <see cref="DateTime?"/>.</param>
        /// <returns></returns>
        public static DateTime? ToSQLSafeDate( this DateTime? dateTime )
        {
            if ( !dateTime.HasValue )
            {
                return null;
            }

            return dateTime.Value.ToSQLSafeDate();
        }
    }
}
