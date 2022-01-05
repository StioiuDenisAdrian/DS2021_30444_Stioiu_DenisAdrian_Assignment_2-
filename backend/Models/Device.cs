using System;
using System.Collections.Generic;

namespace assignment_1_backend.Models
{
    public class Device
    {
        public Guid ID { get; set; }
        public string DeviceName { get; set; }
        public string SensorName { get; set; }
        public string SensorDescription { get; set; }
        public double MAximumValue { get; set; }

        public string UserID { get; set; }
        public virtual ICollection<User> Users { get; set; }
    }
}
