using System.Collections.Generic;

namespace assignment_1_backend.Models.DTOs
{
    public class RegistrationResponseDTO
    {
        public bool IsSuccessfulRegistration { get; set; }
        public IEnumerable<string> Errors { get; set; }
    }
}
