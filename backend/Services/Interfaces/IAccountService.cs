using assignment_1_backend.Models.Data;
using assignment_1_backend.Models.DTOs;
using System.Collections.Generic;

namespace assignment_1_backend.Services.Interfaces
{
    public interface IAccountService
    {
        List<UserData> GetUsers();
        void DeleteUser(string ID);
        UserDTO GetUser(string ID);
        void SaveUser(UserDTO user);
        UserDTO GetCurrentUser(string Name);
    }
}
