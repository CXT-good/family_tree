using Dapper;
using FamilyTreeApi.Models;
using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;

namespace FamilyTreeApi.Controllers;

/// <summary>族谱管理（查询、创建、获取用户族谱）</summary>
[ApiController]
[Route("api/[controller]")]
public class FamilyTreesController : ControllerBase
{
    private readonly string _connectionString;

    public FamilyTreesController(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' is missing.");
    }

    /// <summary>查询族谱（按谱名/姓氏/族谱ID/创建者ID）</summary>
    [HttpPost("search")]
    public async Task<IActionResult> Search([FromBody] FamilyTreeQueryRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Keyword))
            return BadRequest(new FamilyTreeQueryResponse { Success = false, Message = "查询关键词不能为空" });

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        var keyword = request.Keyword.Trim();
        var like = $"%{keyword}%";

        // 谱名/姓氏/族谱ID/创建者ID：数值型 ID 用 CAST 参与 LIKE，避免纯数字同时误解析为 tree_id 与 creator
        const string sql = """
            SELECT
              tree_id AS TreeId,
              tree_name AS TreeName,
              surname AS Surname,
              created_by_user_id AS CreatedByUserId,
              revision_at AS RevisionAt
            FROM family_trees
            WHERE tree_name LIKE @like
               OR surname LIKE @like
               OR CAST(tree_id AS CHAR) LIKE @like
               OR CAST(created_by_user_id AS CHAR) LIKE @like
            ORDER BY revision_at DESC;
            """;

        var trees = await conn.QueryAsync<FamilyTreeDto>(sql, new { like });

        return Ok(new FamilyTreeQueryResponse
        {
            Success = true,
            Trees = trees.ToList()
        });
    }

    /// <summary>创建族谱</summary>
    [HttpPost("create")]
    public async Task<IActionResult> Create([FromBody] FamilyTreeCreateRequest request, [FromQuery] ulong userId)
    {
        if (!ModelState.IsValid)
            return ValidationProblem(ModelState);

        if (string.IsNullOrWhiteSpace(request.TreeName))
            return BadRequest(new FamilyTreeCreateResponse { Success = false, Message = "族谱名不能为空" });

        if (userId == 0)
            return BadRequest(new FamilyTreeCreateResponse { Success = false, Message = "必须提供有效的 userId" });

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        var surname = request.TreeName.Substring(0, 1);
        var now = DateTime.Now;

        const string insertSql = """
            INSERT INTO family_trees (tree_name, surname, created_by_user_id, revision_at)
            VALUES (@treeName, @surname, @userId, @revisionAt);
            """;

        try
        {
            await conn.ExecuteAsync(insertSql, new
            {
                treeName = request.TreeName.Trim(),
                surname,
                userId,
                revisionAt = now
            });

            var treeId = await conn.ExecuteScalarAsync<ulong>("SELECT LAST_INSERT_ID();");

            // 为创建者添加owner角色
            const string managerSql = """
                INSERT INTO tree_managers (tree_id, user_id, role, invited_at)
                VALUES (@treeId, @userId, 'owner', @now);
                """;

            await conn.ExecuteAsync(managerSql, new
            {
                treeId,
                userId,
                now
            });

            return Ok(new FamilyTreeCreateResponse
            {
                Success = true,
                TreeId = treeId,
                TreeName = request.TreeName.Trim(),
                Message = "创建成功"
            });
        }
        catch (MySqlException ex)
        {
            return BadRequest(new FamilyTreeCreateResponse { Success = false, Message = ex.Message });
        }
    }

    /// <summary>获取用户的族谱（我创建的owner + 我管理的editor）</summary>
    [HttpGet("user")]
    public async Task<IActionResult> GetUserFamilyTrees([FromQuery] ulong userId)
    {
        if (userId == 0)
            return BadRequest(new { success = false, message = "必须提供有效的 userId" });

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string sql = """
            SELECT
              ft.tree_id AS TreeId,
              ft.tree_name AS TreeName,
              ft.surname AS Surname,
              ft.created_by_user_id AS CreatedByUserId,
              ft.revision_at AS RevisionAt,
              tm.role AS Role
            FROM tree_managers tm
            JOIN family_trees ft ON tm.tree_id = ft.tree_id
            WHERE tm.user_id = @userId
            ORDER BY ft.revision_at DESC;
            """;

        var trees = await conn.QueryAsync<dynamic>(sql, new { userId });

        var ownedTrees = new List<FamilyTreeDto>();
        var managedTrees = new List<FamilyTreeDto>();

        foreach (var tree in trees)
        {
            var treeDto = new FamilyTreeDto
            {
                TreeId = tree.TreeId,
                TreeName = tree.TreeName,
                Surname = tree.Surname,
                CreatedByUserId = tree.CreatedByUserId,
                RevisionAt = tree.RevisionAt
            };

            if (tree.Role == "owner")
                ownedTrees.Add(treeDto);
            else if (tree.Role == "editor")
                managedTrees.Add(treeDto);
        }

        return Ok(new UserFamilyTreesResponse
        {
            Success = true,
            OwnedTrees = ownedTrees,
            ManagedTrees = managedTrees
        });
    }
}
