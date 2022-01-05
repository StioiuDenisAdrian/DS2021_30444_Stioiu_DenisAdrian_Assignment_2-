using System;
using System.ComponentModel.DataAnnotations;


namespace assignment_1_backend.Models.DTOs
{
    public class DeviceDTO
    {
        public string UserId { get; set; }
        public Guid? Id { get; set; }
        [Required(ErrorMessage = "Name is mandatory", AllowEmptyStrings = false)]
        public string DeviceName { get; set; }
        [Required(ErrorMessage = "Name is mandatory", AllowEmptyStrings = false)]

        public string SensorName { get; set; }
        [Required(ErrorMessage = "Description is mandatory", AllowEmptyStrings = false)]

        public string SensorDescription { get; set; }
        [Required(ErrorMessage = "Maximum value is mandatory")]

        public double MAximumValue { get; set; }

        public static Device ToEntity(DeviceDTO device)
        {
            return new Device
            {
                ID = device.Id.HasValue ? device.Id.Value : Guid.NewGuid(),
                UserID = device.UserId,
                DeviceName = device.DeviceName,
                SensorName = device.SensorName,
                SensorDescription = device.SensorDescription,
                MAximumValue = device.MAximumValue
            };
        }

        public static DeviceDTO ToModel(Device device)
        {
            return new DeviceDTO
            {
                Id = device.ID,
                UserId = device.UserID,
                DeviceName = device.DeviceName,
                SensorName = device.SensorName,
                SensorDescription = device.SensorDescription,
                MAximumValue = device.MAximumValue
            };
        }
    }
}
