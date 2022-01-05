using assignment_1_backend.Models.Data;
using assignment_1_backend.Models.DTOs;
using assignment_1_backend.Services.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;


namespace assignment_1_backend.Controllers
{
    [Route("api/devices")]
    [ApiController]
    public class DeviceController: ControllerBase
    {
        private IDeviceService deviceService { get; set; }

        public DeviceController(IDeviceService deviceService)
        {
            this.deviceService = deviceService;
        }

        [HttpDelete("DeleteDevice/{ID}")]
        [Authorize(Roles = "Manager")]
        public void DeleteDevice(Guid ID)
        {
            deviceService.DeleteDevice(ID);
        }

        [HttpGet("GetDevice/{ID}")]
        [Authorize(Roles = "Manager,Client")]
        public DeviceDTO GetDevice(Guid ID)
        {
            return deviceService.GetDevice(ID);
        }

        [HttpGet("GetDevices/{userId}")]
        [Authorize(Roles = "Manager,Client")]
        public List<DeviceData> GetDevices(string userId)
        {
            return deviceService.GetDevices(userId);
        }

        [HttpPut("UpdateDevice")]
        [Authorize(Roles = "Manager")]
        public void UpdateDevice([FromBody] DeviceDTO device)
        {
            deviceService.SaveDevice(device);
        }

        [HttpPost("SaveDevice")]
        [Authorize(Roles = "Manager")]
        public void SaveDevice([FromBody] DeviceDTO device)
        {
            deviceService.SaveDevice(device);
        }
    }
}
