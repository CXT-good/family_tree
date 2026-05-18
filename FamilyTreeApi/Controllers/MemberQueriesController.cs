using Dapper;
using FamilyTreeApi.Models;
using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;

namespace FamilyTreeApi.Controllers;

/// <summary>成员高级 SQL 查询（每条需求对应一条独立 SQL，列表类支持分页）</summary>
[ApiController]
[Route("api/[controller]")]
public class MemberQueriesController : ControllerBase
{
    private readonly string _connectionString;
    private const int DefaultPageSize = 40;
    private const int MaxPageSize = 100;

    public MemberQueriesController(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' is missing.");
    }

    /// <summary>基本查询：给定成员 ID，查询其配偶及所有子女（单条 SQL，UNION ALL）。</summary>
    [HttpGet("spouse-and-children")]
    public async Task<IActionResult> SpouseAndChildren(
        [FromQuery] ulong treeId,
        [FromQuery] ulong memberId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = DefaultPageSize,
        [FromQuery] bool skipCount = false,
        [FromQuery] int knownTotal = 0)
    {
        if (treeId == 0 || memberId == 0)
            return BadRequest(new MemberAdvancedQueryResponse { Success = false, Message = "必须提供 treeId 与 memberId" });

        const string innerSql = """
            SELECT
              '配偶' AS RelationKind,
              m.member_id AS MemberId,
              m.full_name AS FullName,
              m.gender AS Gender,
              m.generation AS Generation,
              m.birth_date AS BirthDate,
              m.death_date AS DeathDate,
              NULL AS Depth,
              NULL AS AgeYears,
              NULL AS BirthYear,
              NULL AS GenerationAvgBirthYear,
              NULL AS AvgLifespanYears,
              NULL AS MemberCount
            FROM marriages mar
            INNER JOIN members m
              ON m.tree_id = mar.tree_id
             AND m.member_id = (CASE WHEN mar.husband_id = @memberId THEN mar.wife_id ELSE mar.husband_id END)
            WHERE mar.tree_id = @treeId
              AND (mar.husband_id = @memberId OR mar.wife_id = @memberId)
            UNION ALL
            SELECT
              '子女' AS RelationKind,
              c.member_id AS MemberId,
              c.full_name AS FullName,
              c.gender AS Gender,
              c.generation AS Generation,
              c.birth_date AS BirthDate,
              c.death_date AS DeathDate,
              NULL AS Depth,
              NULL AS AgeYears,
              NULL AS BirthYear,
              NULL AS GenerationAvgBirthYear,
              NULL AS AvgLifespanYears,
              NULL AS MemberCount
            FROM members c
            WHERE c.tree_id = @treeId
              AND (c.father_member_id = @memberId OR c.mother_member_id = @memberId)
            """;

        return await RunPagedQueryAsync(
            treeId, memberId, innerSql, "ORDER BY RelationKind, MemberId", page, pageSize, skipCount, knownTotal,
            total => $"共 {total} 条（配偶 + 子女）。");
    }

    /// <summary>统计分析：平均寿命最长的一代人（辈分，单条结果不分页）。</summary>
    [HttpGet("longest-lifespan-generation")]
    public async Task<IActionResult> LongestLifespanGeneration([FromQuery] ulong treeId)
    {
        if (treeId == 0)
            return BadRequest(new MemberAdvancedQueryResponse { Success = false, Message = "必须提供 treeId" });

        const string sql = """
            SELECT
              '统计' AS RelationKind,
              0 AS MemberId,
              CONCAT('第 ', generation, ' 代') AS FullName,
              '' AS Gender,
              generation AS Generation,
              NULL AS BirthDate,
              NULL AS DeathDate,
              NULL AS Depth,
              NULL AS AgeYears,
              NULL AS BirthYear,
              NULL AS GenerationAvgBirthYear,
              avg_lifespan_years AS AvgLifespanYears,
              member_count AS MemberCount
            FROM (
              SELECT
                generation,
                AVG(DATEDIFF(death_date, birth_date) / 365.25) AS avg_lifespan_years,
                COUNT(*) AS member_count
              FROM members
              WHERE tree_id = @treeId
                AND birth_date IS NOT NULL
                AND death_date IS NOT NULL
                AND generation IS NOT NULL
              GROUP BY generation
              ORDER BY avg_lifespan_years DESC
              LIMIT 1
            ) best;
            """;

        await using var conn = await OpenConnectionAsync();
        var rows = (await conn.QueryAsync<MemberQueryRowDto>(sql, new { treeId, memberId = 0UL })).ToList();
        if (rows.Count == 0)
        {
            return Ok(new MemberAdvancedQueryResponse
            {
                Success = true,
                Summary = "该族谱中无同时具备出生、去世日期且标注辈分的成员，无法计算。",
                Total = 0,
                Page = 1,
                PageSize = 1,
                Rows = rows
            });
        }

        var r = rows[0];
        return Ok(new MemberAdvancedQueryResponse
        {
            Success = true,
            Summary = $"平均寿命最长：{r.FullName}，约 {r.AvgLifespanYears:F2} 年（样本 {r.MemberCount} 人）。",
            Total = 1,
            Page = 1,
            PageSize = 1,
            Rows = rows
        });
    }

    /// <summary>年龄超过 50 岁且无配偶的男性成员。</summary>
    [HttpGet("males-over-50-no-spouse")]
    public async Task<IActionResult> MalesOver50NoSpouse(
        [FromQuery] ulong treeId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = DefaultPageSize,
        [FromQuery] bool skipCount = false,
        [FromQuery] int knownTotal = 0)
    {
        if (treeId == 0)
            return BadRequest(new MemberAdvancedQueryResponse { Success = false, Message = "必须提供 treeId" });

        // 用 birth_date 范围代替 TIMESTAMPDIFF，便于走 (tree_id, gender, birth_date) 索引；
        // 拆成两个 NOT EXISTS，分别命中丈夫/妻子索引，避免 OR 导致全表扫描。
        const string filterSql = """
            m.tree_id = @treeId
              AND m.gender = 'M'
              AND m.birth_date IS NOT NULL
              AND m.birth_date <= DATE_SUB(CURDATE(), INTERVAL 50 YEAR)
              AND NOT EXISTS (
                SELECT 1 FROM marriages mar
                WHERE mar.tree_id = @treeId AND mar.husband_id = m.member_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM marriages mar
                WHERE mar.tree_id = @treeId AND mar.wife_id = m.member_id
              )
            """;

        const string countSql = $"SELECT COUNT(*) FROM members m WHERE {filterSql};";

        var dataSql = $"""
            SELECT
              '成员' AS RelationKind,
              m.member_id AS MemberId,
              m.full_name AS FullName,
              m.gender AS Gender,
              m.generation AS Generation,
              m.birth_date AS BirthDate,
              m.death_date AS DeathDate,
              NULL AS Depth,
              TIMESTAMPDIFF(YEAR, m.birth_date, CURDATE()) AS AgeYears,
              NULL AS BirthYear,
              NULL AS GenerationAvgBirthYear,
              NULL AS AvgLifespanYears,
              NULL AS MemberCount
            FROM members m
            WHERE {filterSql}
            ORDER BY m.birth_date ASC, m.member_id
            LIMIT @take OFFSET @skip;
            """;

        return await RunDirectPagedQueryAsync(
            treeId, 0, countSql, dataSql, page, pageSize, skipCount, knownTotal,
            total => $"共 {total} 位年龄超过 50 岁且无配偶的男性。");
    }

    /// <summary>出生年份早于该辈分平均出生年份的成员。</summary>
    [HttpGet("earlier-than-generation-avg-birth")]
    public async Task<IActionResult> EarlierThanGenerationAvgBirth(
        [FromQuery] ulong treeId,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = DefaultPageSize,
        [FromQuery] bool skipCount = false,
        [FromQuery] int knownTotal = 0)
    {
        if (treeId == 0)
            return BadRequest(new MemberAdvancedQueryResponse { Success = false, Message = "必须提供 treeId" });

        const string innerSql = """
            SELECT
              '成员' AS RelationKind,
              m.member_id AS MemberId,
              m.full_name AS FullName,
              m.gender AS Gender,
              m.generation AS Generation,
              m.birth_date AS BirthDate,
              m.death_date AS DeathDate,
              NULL AS Depth,
              NULL AS AgeYears,
              YEAR(m.birth_date) AS BirthYear,
              g.avg_birth_year AS GenerationAvgBirthYear,
              NULL AS AvgLifespanYears,
              NULL AS MemberCount
            FROM members m
            INNER JOIN (
              SELECT
                generation,
                AVG(YEAR(birth_date)) AS avg_birth_year
              FROM members
              WHERE tree_id = @treeId
                AND generation IS NOT NULL
                AND birth_date IS NOT NULL
              GROUP BY generation
            ) g ON g.generation = m.generation
            WHERE m.tree_id = @treeId
              AND m.birth_date IS NOT NULL
              AND m.generation IS NOT NULL
              AND YEAR(m.birth_date) < g.avg_birth_year
            """;

        return await RunPagedQueryAsync(
            treeId, 0, innerSql, "ORDER BY Generation, BirthYear, MemberId", page, pageSize, skipCount, knownTotal,
            total => $"共 {total} 位成员出生年份早于同辈平均分。");
    }

    private async Task<IActionResult> RunPagedQueryAsync(
        ulong treeId,
        ulong memberId,
        string innerSql,
        string orderByClause,
        int page,
        int pageSize,
        bool skipCount,
        int knownTotal,
        Func<int, string> buildSummary)
    {
        page = Math.Max(1, page);
        pageSize = Math.Clamp(pageSize, 1, MaxPageSize);
        var skip = (page - 1) * pageSize;

        await using var conn = await OpenConnectionAsync();
        if (memberId > 0)
        {
            var exists = await conn.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM members WHERE tree_id = @treeId AND member_id = @memberId;",
                new { treeId, memberId });
            if (exists == 0)
                return NotFound(new MemberAdvancedQueryResponse { Success = false, Message = "成员不存在" });
        }

        var countSql = $"SELECT COUNT(*) FROM ({innerSql}) AS _cnt;";
        var dataSql = $"SELECT * FROM ({innerSql}) AS _data {orderByClause} LIMIT @take OFFSET @skip;";
        var param = new { treeId, memberId, take = pageSize, skip };

        var total = await ResolveTotalAsync(conn, countSql, param, page, skipCount, knownTotal);
        var rows = (await conn.QueryAsync<MemberQueryRowDto>(dataSql, param)).ToList();
        return BuildPagedResponse(rows, total, page, pageSize, buildSummary);
    }

    private async Task<IActionResult> RunDirectPagedQueryAsync(
        ulong treeId,
        ulong memberId,
        string countSql,
        string dataSql,
        int page,
        int pageSize,
        bool skipCount,
        int knownTotal,
        Func<int, string> buildSummary)
    {
        page = Math.Max(1, page);
        pageSize = Math.Clamp(pageSize, 1, MaxPageSize);
        var skip = (page - 1) * pageSize;

        await using var conn = await OpenConnectionAsync();
        if (memberId > 0)
        {
            var exists = await conn.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM members WHERE tree_id = @treeId AND member_id = @memberId;",
                new { treeId, memberId });
            if (exists == 0)
                return NotFound(new MemberAdvancedQueryResponse { Success = false, Message = "成员不存在" });
        }

        var param = new { treeId, memberId, take = pageSize, skip };
        var total = await ResolveTotalAsync(conn, countSql, param, page, skipCount, knownTotal);
        var rows = (await conn.QueryAsync<MemberQueryRowDto>(dataSql, param)).ToList();
        return BuildPagedResponse(rows, total, page, pageSize, buildSummary);
    }

    private static async Task<int> ResolveTotalAsync(
        MySqlConnection conn,
        string countSql,
        object param,
        int page,
        bool skipCount,
        int knownTotal)
    {
        if (skipCount && page > 1 && knownTotal > 0)
            return knownTotal;

        return await conn.ExecuteScalarAsync<int>(countSql, param);
    }

    private static IActionResult BuildPagedResponse(
        List<MemberQueryRowDto> rows,
        int total,
        int page,
        int pageSize,
        Func<int, string> buildSummary)
    {
        var totalPages = total == 0 ? 0 : (int)Math.Ceiling(total / (double)pageSize);
        return new OkObjectResult(new MemberAdvancedQueryResponse
        {
            Success = true,
            Total = total,
            Page = page,
            PageSize = pageSize,
            Summary = buildSummary(total) + $" 第 {page}/{Math.Max(totalPages, 1)} 页，本页 {rows.Count} 条。",
            Rows = rows
        });
    }

    private async Task<MySqlConnection> OpenConnectionAsync()
    {
        var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();
        await conn.ExecuteAsync("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci");
        return conn;
    }
}
