using assignment_1_backend.Models;
using System.Collections.Generic;

namespace assignment_1_backend.Repositories.Interfaces
{
    public interface IUserRepository
    {
        IEnumerable<User> GetUsers();
        User GetUser(string ID);
        void SaveUser(User user);
        void DeleteUser(string ID);
        User GetUserByName(string name);
        string GetUserRole(string Id);
    }
}
