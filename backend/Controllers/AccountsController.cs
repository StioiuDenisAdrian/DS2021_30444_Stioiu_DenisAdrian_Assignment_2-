using assignment_1_backend.Models;
using assignment_1_backend.Models.Data;
using assignment_1_backend.Models.DTOs;
using assignment_1_backend.Services;
using assignment_1_backend.Services.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Threading.Tasks;


namespace assignment_1_backend.Controllers
{
    [Route("api/accounts")]
    [ApiController]
    public class AccountsController : ControllerBase
    {
        private readonly UserManager<User> userManager;
        private readonly JwtHandler jwtHandler;
        private readonly IAccountService accountService;


        public AccountsController(UserManager<User> userManager,
                                  JwtHandler jwtHandler,
                                  IAccountService accountService
                                 )
        {
            this.userManager = userManager;
            this.jwtHandler = jwtHandler;
            this.accountService = accountService;

        }

        [HttpPost("Registration")]
        [Authorize(Roles = "Manager")]
        public async Task<IActionResult> RegisterUser([FromBody] UserRegistrationDTO userForRegistration)
        {
            if (userForRegistration == null || !ModelState.IsValid)
            {
                return BadRequest();
            }

            var user = UserRegistrationDTO.ToUser(userForRegistration);
            var result = await userManager.CreateAsync(user, userForRegistration.Password);

            if (!result.Succeeded)
            {
                var errors = result.Errors.Select(e => e.Description);
                return BadRequest(new RegistrationResponseDTO { Errors = errors });
            }

            await userManager.AddToRoleAsync(user, "Client");
            return StatusCode(201);
        }

        [HttpPost("Login")]
        public async Task<IActionResult> Login([FromBody] UserAuthenticationDTO userForAuthentication)
        {
            var user = await userManager.FindByNameAsync(userForAuthentication.UserName);
            if (user == null || !await userManager.CheckPasswordAsync(user, userForAuthentication.Password))
            {
                return Unauthorized(new AuthenticationResponseDTO { ErrorMessage = "Invalid Authentication" });
            }

            var signingCredentials = jwtHandler.GetSigningCredentials();
            var claims = jwtHandler.GetClaims(user);
            var tokenOptions = jwtHandler.GenerateTokenOptions(signingCredentials, claims);
            var token = new JwtSecurityTokenHandler().WriteToken(tokenOptions);

            return Ok(new AuthenticationResponseDTO { IsAuthSuccessful = true, Token = token });
        }

        [HttpGet("GetUsers")]
        [Authorize(Roles = "Manager")]
        public List<UserData> GetUsers()
        {
            return accountService.GetUsers();
        }

        [HttpGet("GetUser/{ID}")]
        [Authorize(Roles = "Manager")]
        public UserDTO GetUser(string ID)
        {
            return accountService.GetUser(ID);
        }

        [HttpDelete("DeleteUser/{ID}")]
        [Authorize(Roles = "Manager")]
        public void DeleteUser(string ID)
        {
            accountService.DeleteUser(ID);
        }


        [HttpPut("SaveUser")]
        [Authorize(Roles = "Manager")]
        public void SaveUser([FromBody] UserDTO user)
        {
            accountService.SaveUser(user);
        }

        [HttpGet("GetCurrentUser/{userName}")]
        [Authorize(Roles = "Manager,Client")]
        public UserDTO GetCurrentUser(string userName)
        {
            return accountService.GetCurrentUser(userName);
        }
    }
}

