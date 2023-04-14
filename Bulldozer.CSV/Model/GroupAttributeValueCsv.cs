using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class GroupAttributeValueCsv
    {
        public string GroupId { get; set; }

        public string AttributeKey { get; set; }

        public string AttributeValue { get; set; }

        public string AttributeValueId { get; set; } = null;

    }
}