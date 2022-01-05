using assignment_1_backend.Models.Data;
using assignment_1_backend.Models.DTOs;
using assignment_1_backend.Repositories.Interfaces;
using assignment_1_backend.Services.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace assignment_1_backend.Services
{
    public class AccountService : IAccountService
    {
        private readonly IUserRepository userRepository;

        public AccountService(IUserRepository userRepository
                              )
        {
            this.userRepository = userRepository;
        }

        public void DeleteUser(string ID)
        {
            userRepository.DeleteUser(ID);
        }

        public UserDTO GetCurrentUser(string Name)
        {
            return UserDTO.FromEntity(userRepository.GetUserByName(Name));
        }

        public UserDTO GetUser(string ID)
        {
            return UserDTO.FromEntity(userRepository.GetUser(ID));
        }

        public List<UserData> GetUsers()
        {
            return userRepository.GetUsers().ToList().Select(u => new UserData
            {
                Id = u.Id,
                Name = u.Name,
                Email  = u.Email,
                Address = u.Address,
                Birthday = u.Birthday,
                Username = u.UserName,
                Role = userRepository.GetUserRole(u.Id)
            }).ToList();
        }

        public void SaveUser(UserDTO user)
        {
            userRepository.SaveUser(UserDTO.FromModel(user));
        }
    }
}
