using assignment_1_backend.Models.DTOs;
using assignment_1_backend.Repositories.Interfaces;
using assignment_1_backend.Services.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace assignment_1_backend.Services
{
    public class MeasurmentService : IMeasurmentService
    {
        private IMeasurmentRepository measurmentRepository { get; set; }

        public MeasurmentService(IMeasurmentRepository measurmentRepository)
        {
            this.measurmentRepository = measurmentRepository;
        }

        public List<MeasurementDTO> GetMeasurments(Guid deviceId, DateTimeOffset dateTime)
        {
            return measurmentRepository.GetMeasurments(deviceId, dateTime).Select(m => MeasurementDTO.ToEntity(m)).ToList();
          
        }
    }
}
