using assignment_1_backend.Models.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace assignment_1_backend.Services.Interfaces
{
   public  interface IMeasurmentService
    {
        List<MeasurementDTO> GetMeasurments(Guid deviceId, DateTimeOffset dateTime);
    }
}
