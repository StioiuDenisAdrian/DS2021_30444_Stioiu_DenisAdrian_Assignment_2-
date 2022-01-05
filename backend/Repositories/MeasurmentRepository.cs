using assignment_1_backend.Models;
using assignment_1_backend.Repositories.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace assignment_1_backend.Repositories
{
    public class MeasurmentRepository : IMeasurmentRepository
    {

        private readonly Context context;

        public MeasurmentRepository(Context context)
        {
            this.context = context;
        }

        public IEnumerable<Measurment> GetMeasurments(Guid deviceId, DateTimeOffset dateTime)
        {
            return context.Measurments.ToList().Where(m => m.DeviceID == deviceId
                                                  && m.TimeStamp.Day == dateTime.Day
                                                  && m.TimeStamp.Month == dateTime.Month
                                                  && m.TimeStamp.Year == dateTime.Year);
        }
    }
}
