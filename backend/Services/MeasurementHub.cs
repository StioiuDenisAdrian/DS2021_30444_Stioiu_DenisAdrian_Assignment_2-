using assignment_1_backend.Models.DTOs;
using assignment_1_backend.Repositories;
using Microsoft.AspNetCore.SignalR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;


namespace assignment_1_backend.Services
{
    public class MeasurementHub: Hub
    {       
        public async Task SendNotification(NotificationDTO notification)
        {
            await Clients.All.SendAsync("maximumValueExcedeed", notification);
        }
    }
}
