using System;
using System.ComponentModel.DataAnnotations;

namespace assignment_1_backend.Models.DTOs
{
    public class UserRegistrationDTO
    {
        [Required(ErrorMessage = "Name is required.")]
        public string Name { get; set; }
        [Required(ErrorMessage = "Username is required.")]
        public string Username { get; set; }
        [Required(ErrorMessage = "Email is required.")]
        public string Email { get; set; }
        [Required(ErrorMessage = "Password is required")]
        public string Password { get; set; }
        [Compare("Password", ErrorMessage = "The password and confirmation password do not match.")]
        public string ConfirmPassword { get; set; }
        [Required(ErrorMessage = "Address is required")]
        public string Address { get; set; }
        [Required(ErrorMessage = "Birthday is required")]
        public DateTime Birthday { get; set; }


        public static User ToUser(UserRegistrationDTO userRegistrationDTO)
        {
            return new User
            {
                Name = userRegistrationDTO.Name,
                Email = userRegistrationDTO.Email,
                UserName = userRegistrationDTO.Username,
                Birthday = userRegistrationDTO.Birthday,
                Address = userRegistrationDTO.Address,
                IsOriginalPasswordChanged = false
            };
        }
    }
}
