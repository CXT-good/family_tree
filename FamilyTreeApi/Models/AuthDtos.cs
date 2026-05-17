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

public class UserTreesResponse
{
    public bool Success { get; set; } = true;
    public List<UserTreeInfo> OwnedTrees { get; set; } = new();
    public List<UserTreeInfo> ManagedTrees { get; set; } = new();
}

public class UserTreeInfo
{
    public ulong TreeId { get; set; }
    public string TreeName { get; set; } = "";
    public string Surname { get; set; } = "";
    public int TotalMembers { get; set; }
    public int MaleCount { get; set; }
    public int FemaleCount { get; set; }
    public string CreateDate { get; set; } = "";
}
