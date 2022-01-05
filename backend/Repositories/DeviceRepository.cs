using assignment_1_backend.Models;
using assignment_1_backend.Repositories.Interfaces;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;

namespace assignment_1_backend.Repositories
{
    public class DeviceRepository : IDeviceRepository
    {

        private readonly Context context;

        public DeviceRepository(Context context)
        {
            this.context = context;
        }

        public void DeleteDevice(Guid id)
        {
            var device = context.Devices.FirstOrDefault(d => d.ID == id);
            context.Devices.Remove(device);
            context.SaveChanges();
        }

        public Device GetDevice(Guid id)
        {
            return context.Devices.FirstOrDefault(d => d.ID == id);
        }

        public IEnumerable<Device> GetDevices(string userId)
        {
            return context.Devices.Where(d => d.UserID == userId);
        }

        public void SaveDevice(Device device)
        {
            var dev = context.Devices.AsTracking().FirstOrDefault(d => d.ID == device.ID);

            if(dev == null)
            {
                dev = new Device();
                dev.ID = device.ID;
                dev.UserID = device.UserID;
                context.Add(dev);
            }

            dev.DeviceName = device.DeviceName;
            dev.SensorName = device.SensorName;
            dev.SensorDescription = device.SensorDescription;
            dev.MAximumValue = device.MAximumValue;
            
            context.SaveChanges();
        }
    }
}
