using System;

namespace assignment_1_backend.Models
{
    public class Measurment
    {
        public Guid ID { get; set; }
        public double KWH { get; set; }
        public DateTimeOffset TimeStamp { get; set; }

        public Guid DeviceID { get; set; }
        public virtual Device Device { get; set; }
    }
}
