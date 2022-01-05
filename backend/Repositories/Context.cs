using assignment_1_backend.Models;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace assignment_1_backend.Repositories
{
    public class Context : IdentityDbContext<User>
    {
        public Context(DbContextOptions<Context> options)
            : base(options)
        {

        }

        public DbSet<Device> Devices { get; set; }
        public DbSet<Measurment> Measurments{get;set;}
       
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.ApplyConfiguration(new RoleConfiguration());
            modelBuilder.Entity<Measurment>();
            modelBuilder.Entity<Device>();
        }
    }
}
