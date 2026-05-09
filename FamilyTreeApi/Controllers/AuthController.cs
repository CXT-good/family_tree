using Dapper;
using FamilyTreeApi.Models;
using FamilyTreeApi.Services;
using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;

namespace FamilyTreeApi.Controllers;

file sealed class LoginRow
{
    public ulong UserId { get; init; }
    public string Username { get; init; } = "";
    public string PasswordHash { get; init; } = "";
}

/// <summary>用户注册与登录（原生 SQL + Dapper）</summary>
[ApiController]
[Route("api/[controller]")]
public class AuthController : ControllerBase
{
    private readonly string _connectionString;

    public AuthController(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' is missing.");
    }

    /// <summary>注册：用户名唯一，密码存 SHA256 十六进制。</summary>
    [HttpPost("register")]
    public async Task<IActionResult> Register([FromBody] RegisterRequest body)
    {
        if (!ModelState.IsValid)
            return ValidationProblem(ModelState);

        var hash = PasswordHasher.Sha256Hex(body.Password);
        var now = DateTime.Now;

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string insertSql = """
            INSERT INTO users (username, password_hash, registered_at)
            VALUES (@username, @hash, @registeredAt);
            """;

        try
        {
            await conn.ExecuteAsync(insertSql, new
            {
                username = body.Username.Trim(),
                hash,
                registeredAt = now,
            });
            var id = await conn.ExecuteScalarAsync<ulong>("SELECT LAST_INSERT_ID();");

            return Ok(new AuthOkResponse
            {
                UserId = id,
                Username = body.Username.Trim(),
            });
        }
        catch (MySqlException ex) when (ex.Number == 1062)
        {
            return Conflict(new { success = false, message = "用户名已被占用" });
        }
    }

    /// <summary>登录：校验用户名与密码哈希是否与库中一致。</summary>
    [HttpPost("login")]
    public async Task<IActionResult> Login([FromBody] LoginRequest body)
    {
        if (!ModelState.IsValid)
            return ValidationProblem(ModelState);

        var hash = PasswordHasher.Sha256Hex(body.Password);

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string sql = """
            SELECT user_id AS UserId, username AS Username, password_hash AS PasswordHash
            FROM users
            WHERE username = @username
            LIMIT 1;
            """;

        var row = await conn.QueryFirstOrDefaultAsync<LoginRow>(sql, new { username = body.Username.Trim() });

        if (row is null || row.PasswordHash != hash)
            return Unauthorized(new { success = false, message = "用户名或密码错误" });

        return Ok(new AuthOkResponse
        {
            UserId = row.UserId,
            Username = row.Username,
        });
    }
}
