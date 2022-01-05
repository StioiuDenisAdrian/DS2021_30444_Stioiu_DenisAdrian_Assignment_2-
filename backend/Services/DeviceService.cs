using assignment_1_backend.Models.Data;
using assignment_1_backend.Models.DTOs;
using assignment_1_backend.Repositories.Interfaces;
using assignment_1_backend.Services.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace assignment_1_backend.Services
{
    public class DeviceService : IDeviceService
    {

       private  IDeviceRepository deviceRepository { get; set; }

        public DeviceService(IDeviceRepository deviceRepository)
        {
            this.deviceRepository = deviceRepository;
        }
        public void DeleteDevice(Guid id)
        {
            deviceRepository.DeleteDevice(id);
        }

        public DeviceDTO GetDevice(Guid id)
        {
            return DeviceDTO.ToModel(deviceRepository.GetDevice(id));
        }

        public List<DeviceData> GetDevices(string userId)
        {
            return deviceRepository.GetDevices(userId).Select(d => new DeviceData
            {
                Id = d.ID,
                DeviceName = d.DeviceName,
                MAximumValue = d.MAximumValue
            }).ToList();
        }

        public void SaveDevice(DeviceDTO device)
        {
            deviceRepository.SaveDevice(DeviceDTO.ToEntity(device));
        }
    }
}
