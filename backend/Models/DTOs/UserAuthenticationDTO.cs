using System.ComponentModel.DataAnnotations;

namespace assignment_1_backend.Models.DTOs
{
    public class UserAuthenticationDTO
    {
        [Required(ErrorMessage = "Email is required.")]
        public string UserName { get; set; }
        [Required(ErrorMessage = "Email is required.")]
        public string Password { get; set; }
    }
}
