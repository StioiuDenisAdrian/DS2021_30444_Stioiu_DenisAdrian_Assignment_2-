using Microsoft.AspNetCore.Identity;
using System;

namespace assignment_1_backend.Models
{
    public class User: IdentityUser
    {
        public string Name { get; set; }
        public DateTime Birthday { get; set; }
        public string Address { get; set; }
        public bool IsOriginalPasswordChanged { get; set; }

    }
}
