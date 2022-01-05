using assignment_1_backend.Models.Data;
using assignment_1_backend.Models.DTOs;
using System;
using System.Collections.Generic;

namespace assignment_1_backend.Services.Interfaces
{
    public interface IDeviceService
    {
        List<DeviceData> GetDevices(string userId);
        void DeleteDevice(Guid id);
        DeviceDTO GetDevice(Guid id);
        void SaveDevice(DeviceDTO device);
    }
}
