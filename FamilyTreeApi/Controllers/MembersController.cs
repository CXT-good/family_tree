using Dapper;
using FamilyTreeApi.Models;
using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;

namespace FamilyTreeApi.Controllers;

/// <summary>族谱成员 CRUD（原生 SQL + Dapper）</summary>
[ApiController]
[Route("api/[controller]")]
public class MembersController : ControllerBase
{
    private readonly string _connectionString;

    public MembersController(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' is missing.");
    }

    /// <summary>分页列表：必须指定 tree_id；可选姓名关键字。</summary>
    [HttpGet]
    public async Task<IActionResult> List([FromQuery] MemberListQuery query)
    {
        if (query.TreeId == 0)
            return BadRequest(new { message = "必须提供有效的 treeId" });

        if (!ModelState.IsValid)
            return ValidationProblem(ModelState);

        var offset = (query.Page - 1) * query.PageSize;
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string countSql = """
            SELECT COUNT(*)
            FROM members
            WHERE tree_id = @treeId
              AND (@keyword IS NULL OR @keyword = '' OR full_name LIKE @like);
            """;

        const string listSql = """
            SELECT
              member_id AS MemberId,
              tree_id AS TreeId,
              full_name AS FullName,
              gender AS Gender,
              birth_date AS BirthDate,
              death_date AS DeathDate,
              biography AS Biography,
              father_member_id AS FatherMemberId,
              mother_member_id AS MotherMemberId,
              generation AS Generation
            FROM members
            WHERE tree_id = @treeId
              AND (@keyword IS NULL OR @keyword = '' OR full_name LIKE @like)
            ORDER BY member_id
            LIMIT @take OFFSET @skip;
            """;

        var kw = string.IsNullOrWhiteSpace(query.Keyword) ? null : query.Keyword.Trim();
        var like = kw is null ? null : $"%{kw}%";

        var total = await conn.ExecuteScalarAsync<int>(countSql, new
        {
            treeId = query.TreeId,
            keyword = kw,
            like,
        });

        var items = await conn.QueryAsync<MemberDto>(listSql, new
        {
            treeId = query.TreeId,
            keyword = kw,
            like,
            take = query.PageSize,
            skip = offset,
        });

        return Ok(new
        {
            success = true,
            total,
            query.Page,
            query.PageSize,
            items,
        });
    }

    /// <summary>按成员主键查询（可选 treeId 校验归属）</summary>
    [HttpGet("{memberId:long}")]
    public async Task<IActionResult> Get(ulong memberId, [FromQuery] ulong? treeId)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string sql = """
            SELECT
              member_id AS MemberId,
              tree_id AS TreeId,
              full_name AS FullName,
              gender AS Gender,
              birth_date AS BirthDate,
              death_date AS DeathDate,
              biography AS Biography,
              father_member_id AS FatherMemberId,
              mother_member_id AS MotherMemberId,
              generation AS Generation
            FROM members
            WHERE member_id = @memberId
              AND (@treeId IS NULL OR tree_id = @treeId)
            LIMIT 1;
            """;

        var row = await conn.QueryFirstOrDefaultAsync<MemberDto>(sql, new { memberId, treeId });

        if (row is null)
            return NotFound(new { success = false, message = "成员不存在" });

        return Ok(new { success = true, data = row });
    }

    /// <summary>新增成员</summary>
    [HttpPost]
    public async Task<IActionResult> Create([FromBody] MemberCreateRequest body)
    {
        if (!ModelState.IsValid)
            return ValidationProblem(ModelState);

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string sql = """
            INSERT INTO members (
              tree_id, full_name, gender, birth_date, death_date, biography,
              father_member_id, mother_member_id, generation
            )
            VALUES (
              @treeId, @fullName, @gender, @birthDate, @deathDate, @biography,
              @fatherMemberId, @motherMemberId, @generation
            );
            """;

        try
        {
            await conn.ExecuteAsync(sql, new
            {
                treeId = body.TreeId,
                fullName = body.FullName,
                gender = body.Gender,
                birthDate = body.BirthDate,
                deathDate = body.DeathDate,
                biography = body.Biography,
                fatherMemberId = body.FatherMemberId,
                motherMemberId = body.MotherMemberId,
                generation = body.Generation,
            });

            var newId = await conn.ExecuteScalarAsync<ulong>("SELECT LAST_INSERT_ID();");
            return CreatedAtAction(nameof(Get), new { memberId = newId }, new { success = true, memberId = newId });
        }
        catch (MySqlException ex)
        {
            return BadRequest(new { success = false, message = ex.Message });
        }
    }

    /// <summary>更新成员（路径中带 treeId，防止误改其它谱）</summary>
    [HttpPut("{treeId:long}/{memberId:long}")]
    public async Task<IActionResult> Update(ulong treeId, ulong memberId, [FromBody] MemberUpdateRequest body)
    {
        if (!ModelState.IsValid)
            return ValidationProblem(ModelState);

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string sql = """
            UPDATE members
            SET full_name = @fullName,
                gender = @gender,
                birth_date = @birthDate,
                death_date = @deathDate,
                biography = @biography,
                father_member_id = @fatherMemberId,
                mother_member_id = @motherMemberId,
                generation = @generation
            WHERE member_id = @memberId AND tree_id = @treeId;
            """;

        try
        {
            var n = await conn.ExecuteAsync(sql, new
            {
                treeId,
                memberId,
                fullName = body.FullName,
                gender = body.Gender,
                birthDate = body.BirthDate,
                deathDate = body.DeathDate,
                biography = body.Biography,
                fatherMemberId = body.FatherMemberId,
                motherMemberId = body.MotherMemberId,
                generation = body.Generation,
            });

            if (n == 0)
                return NotFound(new { success = false, message = "未找到对应成员或 treeId 不匹配" });

            return Ok(new { success = true, updated = n });
        }
        catch (MySqlException ex)
        {
            return BadRequest(new { success = false, message = ex.Message });
        }
    }

    /// <summary>删除成员</summary>
    [HttpDelete("{treeId:long}/{memberId:long}")]
    public async Task<IActionResult> Delete(ulong treeId, ulong memberId)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string sql = """
            DELETE FROM members
            WHERE member_id = @memberId AND tree_id = @treeId;
            """;

        try
        {
            var n = await conn.ExecuteAsync(sql, new { treeId, memberId });
            if (n == 0)
                return NotFound(new { success = false, message = "未找到对应成员" });

            return Ok(new { success = true, deleted = n });
        }
        catch (MySqlException ex) when (ex.Number == 1451)
        {
            return Conflict(new { success = false, message = "存在其它记录引用该成员，无法删除" });
        }
    }
}
