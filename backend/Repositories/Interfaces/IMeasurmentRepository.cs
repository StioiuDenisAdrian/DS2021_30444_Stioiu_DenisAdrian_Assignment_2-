using assignment_1_backend.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace assignment_1_backend.Repositories.Interfaces
{
    public interface IMeasurmentRepository
    {
        IEnumerable<Measurment> GetMeasurments(Guid deviceId, DateTimeOffset dateTime);
    }
}
