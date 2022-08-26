using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bulldozer.Model
{
    public class AttendanceImport
    {
        public string AttendanceForeignId { get; set; }

        public string PersonForeignId { get; set; }

        public string GroupForeignId { get; set; }

        public string LocationForeignId { get; set; }

        public string ScheduleForeignId { get; set; }

        public DateTime StartDateTime { get; set; }

        public DateTime? EndDateTime { get; set; }

        public string Note { get; set; }

        public int? CampusId { get; set; }
    }
}
