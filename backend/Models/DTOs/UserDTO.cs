using System;
using System.ComponentModel.DataAnnotations;

namespace assignment_1_backend.Models.DTOs
{
    public class UserDTO
    {
        public string Id { get; set; }
        [Required(ErrorMessage = "Name cannot be empty", AllowEmptyStrings = false)]
        public string Name { get; set; }
        [Required(ErrorMessage = "Birthday is mandatory")]
        public DateTime Birthday { get; set; }
        [Required(ErrorMessage = "Address is mandatory", AllowEmptyStrings = false)]
        public string Address { get; set; }
        [Required(ErrorMessage = "Email is mandatory", AllowEmptyStrings = false)]
        public string Email { get; set; }


        public static UserDTO FromEntity(User user)
        {
            return new UserDTO
            {
                Id = user.Id,
                Name = user.Name,
                Birthday = user.Birthday,
                Address = user.Address,
                Email = user.Email
            };
        }

        public static User FromModel(UserDTO userDTO)
        {
            return new User
            {
                Id =userDTO.Id,
                Name = userDTO.Name,
                Birthday = userDTO.Birthday,
                Address = userDTO.Address,
                Email = userDTO.Email
            };
        }
    }
}
