using assignment_1_backend.Models;
using assignment_1_backend.Models.DTOs;
using assignment_1_backend.Repositories;
using assignment_1_backend.Services.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;

namespace assignment_1_backend.Controllers
{
    [Route("api/measurements")]
    [ApiController]
    public class MeasurementController : ControllerBase
    {
        private IMeasurmentService measurmentService { get; set; }
        private Context context { get; set; }

        public MeasurementController(IMeasurmentService measurmentService,
            Context context)
        {
            this.measurmentService = measurmentService;
            this.context = context;
        }


        [HttpPost("GetMeasurements/{deviceID}")]
        [Authorize(Roles = "Manager,Client")]
        public List<MeasurementDTO> GetMeasurements(Guid deviceID,[FromBody] string dateTime)
        {
            var date = DateTimeOffset.Parse(dateTime).LocalDateTime;
            return measurmentService.GetMeasurments(deviceID, date);
        }

        //[HttpPost]
        //public void InsertData()
        //{

        //    int hour = 1;
        //    for (int i = 0; i <= 23; i++)
        //    {

        //        DateTime sourceDate = new DateTime(2021, 11, 3, hour + i, 30, 0);
        //        DateTimeOffset targetTime;
        //        DateTime utcTime = DateTime.SpecifyKind(sourceDate, DateTimeKind.Utc);
        //        targetTime = new DateTimeOffset(utcTime);
        //        Random rnd = new Random();
        //        double kwh = rnd.Next(1000, 13000);

        //        context.Measurments.Add(new Measurment
        //        {
        //            KWH = kwh,
        //            TimeStamp = targetTime,
        //            DeviceID = Guid.Parse("ce693c40-d878-4c24-a6ac-307634634f82")
        //        });
        //        context.SaveChanges();
        //    }
        //}
    }
}

