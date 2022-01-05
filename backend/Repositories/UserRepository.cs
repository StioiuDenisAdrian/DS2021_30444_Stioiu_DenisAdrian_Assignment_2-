using assignment_1_backend.Models;
using assignment_1_backend.Repositories.Interfaces;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;

namespace assignment_1_backend.Repositories
{
    public class UserRepository : IUserRepository
    {
        private readonly Context context;

        public UserRepository(Context context)
        {
            this.context = context;
        }

        public void DeleteUser(string ID)
        {
            var user = context.Users.FirstOrDefault(u => u.Id == ID);
            context.Users.Remove(user);
            context.SaveChanges();
        }

        public User GetUser(string ID)
        {
            return context.Users.FirstOrDefault(u => u.Id == ID);
        }

        public User GetUserByName(string name)
        {
            return context.Users.FirstOrDefault(u => u.UserName == name);
        }

        public string GetUserRole(string Id)
        {
            return context.Roles.FirstOrDefault(r => r.Id == context.UserRoles.FirstOrDefault(ur => ur.UserId == Id).RoleId).Name;
        }

        public IEnumerable<User> GetUsers()
        {
            var users = context.Users;
            return users;
        }


        public void SaveUser(User user)
        {
            var dbUser = context.Users.AsTracking().FirstOrDefault(u => u.Id == user.Id);
            dbUser.Name = user.Name;
            dbUser.Email = user.Email;
            dbUser.Address = user.Address;
            dbUser.Birthday = user.Birthday;
            context.SaveChanges();
        }
    }
}
