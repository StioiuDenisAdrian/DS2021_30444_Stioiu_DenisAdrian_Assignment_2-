using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace assignment_1_backend.Models.DTOs
{
    public class MeasurementDTO
    {
        public double KWH { get; set; }
        public DateTimeOffset TimeStamp { get; set; }


        public static MeasurementDTO ToEntity(Measurment measurment)
        {
            return new MeasurementDTO
            {
                KWH = measurment.KWH,
                TimeStamp = measurment.TimeStamp
            };
        }
    }
}
