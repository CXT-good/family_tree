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

file sealed class UserTreeRow
{
    public ulong TreeId { get; init; }
    public string TreeName { get; init; } = "";
    public string Surname { get; init; } = "";
    public DateTime CreateDate { get; init; }
    public string Role { get; init; } = "";
    public int TotalMembers { get; init; }
    public int MaleCount { get; init; }
    public int FemaleCount { get; init; }
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

    /// <summary>获取用户的族谱列表（owner为创建的，editor为管理的）</summary>
    [HttpGet("trees")]
    public async Task<IActionResult> GetUserTrees([FromQuery] ulong userId)
    {
        if (userId == 0)
            return BadRequest(new { success = false, message = "必须提供有效的 userId" });

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        // 查询用户管理的族谱
        const string sql = """
            SELECT
              tm.tree_id AS TreeId,
              ft.tree_name AS TreeName,
              ft.surname AS Surname,
              ft.revision_at AS CreateDate,
              tm.role AS Role,
              COUNT(m.member_id) AS TotalMembers,
              SUM(CASE WHEN m.gender = 'M' THEN 1 ELSE 0 END) AS MaleCount,
              SUM(CASE WHEN m.gender = 'F' THEN 1 ELSE 0 END) AS FemaleCount
            FROM tree_managers tm
            JOIN family_trees ft ON tm.tree_id = ft.tree_id
            LEFT JOIN members m ON ft.tree_id = m.tree_id
            WHERE tm.user_id = @userId
            GROUP BY tm.tree_id, ft.tree_name, ft.surname, ft.revision_at, tm.role
            ORDER BY ft.revision_at DESC;
            """;

        var trees = await conn.QueryAsync<UserTreeRow>(sql, new { userId });

        var ownedTrees = new List<UserTreeInfo>();
        var managedTrees = new List<UserTreeInfo>();

        foreach (var tree in trees)
        {
            var info = new UserTreeInfo
            {
                TreeId = tree.TreeId,
                TreeName = tree.TreeName,
                Surname = tree.Surname,
                TotalMembers = tree.TotalMembers,
                MaleCount = tree.MaleCount,
                FemaleCount = tree.FemaleCount,
                CreateDate = tree.CreateDate.ToString("yyyy-MM")
            };

            if (tree.Role == "owner")
                ownedTrees.Add(info);
            else if (tree.Role == "editor")
                managedTrees.Add(info);
        }

        return Ok(new UserTreesResponse
        {
            OwnedTrees = ownedTrees,
            ManagedTrees = managedTrees
        });
    }
}
