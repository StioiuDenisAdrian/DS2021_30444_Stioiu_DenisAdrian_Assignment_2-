using assignment_1_backend.Models;
using System;
using System.Collections.Generic;

namespace assignment_1_backend.Repositories.Interfaces
{
    public interface IDeviceRepository
    {
        IEnumerable<Device> GetDevices(string userId);
        Device GetDevice(Guid id);
        void SaveDevice(Device device);
        void DeleteDevice(Guid id);
    }
}
