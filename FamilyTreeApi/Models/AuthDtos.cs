using System.ComponentModel.DataAnnotations;

namespace FamilyTreeApi.Models;

public class RegisterRequest
{
    [Required, MinLength(3), MaxLength(64)]
    public string Username { get; set; } = "";

    [Required, MinLength(6), MaxLength(128)]
    public string Password { get; set; } = "";
}

public class LoginRequest
{
    [Required]
    public string Username { get; set; } = "";

    [Required]
    public string Password { get; set; } = "";
}

public class AuthOkResponse
{
    public bool Success { get; set; } = true;
    public ulong UserId { get; set; }
    public string Username { get; set; } = "";
}
