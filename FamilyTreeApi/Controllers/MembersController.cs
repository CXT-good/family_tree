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

    /// <summary>分支预览：按当前成员向下展示其后代关系（仅加载该分支子树，默认最多 4 代）</summary>
    [HttpGet("branch")]
    public async Task<IActionResult> Branch(
        [FromQuery] ulong treeId,
        [FromQuery] ulong rootMemberId,
        [FromQuery] int maxDepth = 4)
    {
        if (treeId == 0 || rootMemberId == 0)
            return BadRequest(new { success = false, message = "必须提供有效的 treeId 和 rootMemberId" });

        if (maxDepth < 1 || maxDepth > 20)
            maxDepth = 4;

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        var root = await LoadMemberAsync(conn, treeId, rootMemberId);
        if (root is null)
            return NotFound(new { success = false, message = "未找到根成员" });

        var members = await LoadDescendantsAsync(conn, treeId, root, maxDepth);
        var childrenMap = BuildChildrenMap(members);
        var node = BuildTreeNode(root, childrenMap, maxDepth: maxDepth);

        return Ok(new MemberTreeResponse { Success = true, Data = node });
    }

    /// <summary>祖先查询：显示当前成员的父母与更高父辈（仅沿父系/母系向上加载）</summary>
    [HttpGet("ancestors")]
    public async Task<IActionResult> Ancestors([FromQuery] ulong treeId, [FromQuery] ulong memberId)
    {
        if (treeId == 0 || memberId == 0)
            return BadRequest(new { success = false, message = "必须提供有效的 treeId 和 memberId" });

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        var member = await LoadMemberAsync(conn, treeId, memberId);
        if (member is null)
            return NotFound(new { success = false, message = "未找到成员" });

        var membersById = await LoadAncestorsAsync(conn, treeId, member);
        var node = BuildAncestorTree(member, membersById);

        return Ok(new MemberTreeResponse { Success = true, Data = node });
    }

    /// <summary>按成员主键查询（可选 treeId 校验归属）</summary>
    [HttpGet("{memberId:long}")]
    public async Task<IActionResult> Get(ulong memberId, [FromQuery] ulong? treeId)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        var row = await LoadMemberAsync(conn, treeId ?? 0, memberId, requireTreeMatch: treeId.HasValue);
        if (row is null)
            return NotFound(new { success = false, message = "成员不存在" });

        return Ok(new { success = true, data = row });
    }

    /// <summary>亲缘关系查询：判断两人之间是否存在血缘/婚姻连通路径</summary>
    [HttpGet("relationship")]
    public async Task<IActionResult> Relationship([FromQuery] ulong treeId, [FromQuery] ulong memberId1, [FromQuery] ulong memberId2)
    {
        if (treeId == 0 || memberId1 == 0 || memberId2 == 0)
            return BadRequest(new { success = false, message = "必须提供有效的 treeId、memberId1 和 memberId2" });

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();

        const string memberSql = """
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
            WHERE tree_id = @treeId;
            """;

        var members = (await conn.QueryAsync<MemberDto>(memberSql, new { treeId })).ToList();
        var memberById = members.ToDictionary(m => m.MemberId);
        if (!memberById.ContainsKey(memberId1) || !memberById.ContainsKey(memberId2))
            return NotFound(new { success = false, message = "指定成员在该族谱中不存在" });

        const string marriageSql = """
            SELECT husband_id AS HusbandId, wife_id AS WifeId
            FROM marriages
            WHERE tree_id = @treeId;
            """;

        var marriages = await conn.QueryAsync<(ulong HusbandId, ulong WifeId)>(marriageSql, new { treeId });
        var graph = BuildRelationshipGraph(members, marriages);

        var path = FindRelationshipPath(memberId1, memberId2, graph, memberById);
        if (path == null)
            return Ok(new MemberRelationshipResponse { Success = false, Message = "未找到亲缘关系通路" });

        var result = path.Select((entry, index) => new MemberRelationNodeDto
        {
            MemberId = entry.Member.MemberId,
            FullName = entry.Member.FullName,
            Gender = entry.Member.Gender,
            Generation = entry.Member.Generation,
            RelationToPrevious = index == 0 ? "起点" : entry.RelationLabel
        }).ToList();

        return Ok(new MemberRelationshipResponse { Success = true, Path = result });
    }

    private const string MemberSelectSql = """
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
        """;

    private static Task<MemberDto?> LoadMemberAsync(MySqlConnection conn, ulong treeId, ulong memberId, bool requireTreeMatch = true)
    {
        var sql = requireTreeMatch
            ? MemberSelectSql + " WHERE member_id = @memberId AND tree_id = @treeId LIMIT 1;"
            : MemberSelectSql + " WHERE member_id = @memberId AND (@treeId = 0 OR tree_id = @treeId) LIMIT 1;";

        return conn.QueryFirstOrDefaultAsync<MemberDto>(sql, new { memberId, treeId });
    }

    /// <summary>自根成员起 BFS 加载直系后代（不扫描整本族谱，受 maxDepth 限制）。</summary>
    private static async Task<List<MemberDto>> LoadDescendantsAsync(MySqlConnection conn, ulong treeId, MemberDto root, int maxDepth)
    {
        const int maxNodes = 1500;
        var membersById = new Dictionary<ulong, MemberDto> { [root.MemberId] = root };
        var frontier = new List<ulong> { root.MemberId };
        var currentDepth = 0;

        var childrenSql = MemberSelectSql + """
             WHERE tree_id = @treeId
               AND (father_member_id IN @parentIds OR mother_member_id IN @parentIds);
            """;

        while (frontier.Count > 0 && membersById.Count < maxNodes && currentDepth < maxDepth)
        {
            var children = (await conn.QueryAsync<MemberDto>(childrenSql, new { treeId, parentIds = frontier })).ToList();
            frontier.Clear();
            currentDepth++;

            foreach (var child in children)
            {
                if (!membersById.TryAdd(child.MemberId, child))
                    continue;
                frontier.Add(child.MemberId);
            }
        }

        return membersById.Values.ToList();
    }

    /// <summary>按辈分逐层批量向上加载祖先（不扫描整本族谱）。</summary>
    private static async Task<Dictionary<ulong, MemberDto>> LoadAncestorsAsync(MySqlConnection conn, ulong treeId, MemberDto member)
    {
        var membersById = new Dictionary<ulong, MemberDto> { [member.MemberId] = member };
        var pending = new HashSet<ulong>();

        void EnqueueParent(ulong? parentId)
        {
            if (parentId is > 0 && !membersById.ContainsKey(parentId.Value))
                pending.Add(parentId.Value);
        }

        EnqueueParent(member.FatherMemberId);
        EnqueueParent(member.MotherMemberId);

        var ancestorsSql = MemberSelectSql + " WHERE tree_id = @treeId AND member_id IN @memberIds;";

        while (pending.Count > 0)
        {
            var ids = pending.ToList();
            pending.Clear();

            var loaded = (await conn.QueryAsync<MemberDto>(ancestorsSql, new { treeId, memberIds = ids })).ToList();
            foreach (var parent in loaded)
            {
                membersById[parent.MemberId] = parent;
                EnqueueParent(parent.FatherMemberId);
                EnqueueParent(parent.MotherMemberId);
            }
        }

        return membersById;
    }

    private static Dictionary<ulong, List<MemberDto>> BuildChildrenMap(List<MemberDto> members)
    {
        var map = new Dictionary<ulong, HashSet<ulong>>();
        foreach (var member in members)
        {
            if (member.FatherMemberId.HasValue)
            {
                var fatherId = member.FatherMemberId.Value;
                if (!map.ContainsKey(fatherId))
                    map[fatherId] = new HashSet<ulong>();
                map[fatherId].Add(member.MemberId);
            }

            if (member.MotherMemberId.HasValue)
            {
                var motherId = member.MotherMemberId.Value;
                if (!map.ContainsKey(motherId))
                    map[motherId] = new HashSet<ulong>();
                map[motherId].Add(member.MemberId);
            }
        }

        var membersById = members.ToDictionary(m => m.MemberId);
        var result = new Dictionary<ulong, List<MemberDto>>();
        foreach (var kvp in map)
        {
            result[kvp.Key] = kvp.Value
                .Where(id => membersById.ContainsKey(id))
                .Select(id => membersById[id])
                .ToList();
        }
        return result;
    }

    private static MemberTreeNodeDto BuildTreeNode(
        MemberDto member,
        Dictionary<ulong, List<MemberDto>> childrenMap,
        int depth = 0,
        int maxDepth = 50)
    {
        var node = new MemberTreeNodeDto
        {
            MemberId = member.MemberId,
            TreeId = member.TreeId,
            FullName = member.FullName,
            Gender = member.Gender,
            BirthDate = member.BirthDate,
            DeathDate = member.DeathDate,
            Biography = member.Biography,
            FatherMemberId = member.FatherMemberId,
            MotherMemberId = member.MotherMemberId,
            Generation = member.Generation,
            Relation = depth == 0 ? "当前成员" : "子女",
            Children = new List<MemberTreeNodeDto>()
        };

        if (depth >= maxDepth)
            return node;

        if (childrenMap.TryGetValue(member.MemberId, out var children))
        {
            var distinctChildren = children.DistinctBy(c => c.MemberId).OrderBy(c => c.FullName).ToList();
            foreach (var child in distinctChildren)
            {
                node.Children.Add(BuildTreeNode(child, childrenMap, depth + 1, maxDepth));
            }
        }

        return node;
    }

    private static MemberTreeNodeDto BuildAncestorTree(MemberDto member, Dictionary<ulong, MemberDto> membersById, int depth = 0)
    {
        var node = new MemberTreeNodeDto
        {
            MemberId = member.MemberId,
            TreeId = member.TreeId,
            FullName = member.FullName,
            Gender = member.Gender,
            BirthDate = member.BirthDate,
            DeathDate = member.DeathDate,
            Biography = member.Biography,
            FatherMemberId = member.FatherMemberId,
            MotherMemberId = member.MotherMemberId,
            Generation = member.Generation,
            Relation = depth == 0 ? "当前成员" : "祖先",
            Children = new List<MemberTreeNodeDto>()
        };

        if (depth >= 50)
            return node;

        if (member.FatherMemberId.HasValue && membersById.TryGetValue(member.FatherMemberId.Value, out var father))
        {
            var fatherNode = BuildAncestorTree(father, membersById, depth + 1);
            fatherNode.Relation = "父亲";
            node.Children.Add(fatherNode);
        }

        if (member.MotherMemberId.HasValue && membersById.TryGetValue(member.MotherMemberId.Value, out var mother))
        {
            var motherNode = BuildAncestorTree(mother, membersById, depth + 1);
            motherNode.Relation = "母亲";
            node.Children.Add(motherNode);
        }

        return node;
    }

    private static Dictionary<ulong, List<(ulong Target, string Relation)>> BuildRelationshipGraph(List<MemberDto> members, IEnumerable<(ulong HusbandId, ulong WifeId)> marriages)
    {
        var graph = new Dictionary<ulong, List<(ulong Target, string Relation)>>();
        foreach (var member in members)
            graph[member.MemberId] = new List<(ulong, string)>();

        foreach (var member in members)
        {
            if (member.FatherMemberId.HasValue)
            {
                graph[member.MemberId].Add((member.FatherMemberId.Value, "父亲"));
                graph[member.FatherMemberId.Value].Add((member.MemberId, "子女"));
            }

            if (member.MotherMemberId.HasValue)
            {
                graph[member.MemberId].Add((member.MotherMemberId.Value, "母亲"));
                graph[member.MotherMemberId.Value].Add((member.MemberId, "子女"));
            }
        }

        foreach (var marriage in marriages)
        {
            if (graph.ContainsKey(marriage.HusbandId) && graph.ContainsKey(marriage.WifeId))
            {
                graph[marriage.HusbandId].Add((marriage.WifeId, "配偶"));
                graph[marriage.WifeId].Add((marriage.HusbandId, "配偶"));
            }
        }

        return graph;
    }

    private static List<(MemberDto Member, string RelationLabel)>? FindRelationshipPath(ulong startId, ulong targetId, Dictionary<ulong, List<(ulong Target, string Relation)>> graph, Dictionary<ulong, MemberDto> memberById)
    {
        var queue = new Queue<ulong>();
        var visited = new Dictionary<ulong, (ulong? Previous, string Relation)>();
        queue.Enqueue(startId);
        visited[startId] = (null, "起点");

        while (queue.Count > 0)
        {
            var currentId = queue.Dequeue();
            if (!graph.TryGetValue(currentId, out var neighbors))
                continue;

            foreach (var (neighborId, relation) in neighbors)
            {
                if (visited.ContainsKey(neighborId))
                    continue;

                visited[neighborId] = (currentId, relation);
                queue.Enqueue(neighborId);

                if (neighborId == targetId)
                    break;
            }

            if (visited.ContainsKey(targetId))
                break;
        }

        if (!visited.ContainsKey(targetId))
            return null;

        var path = new List<(MemberDto Member, string RelationLabel)>();
        var current = targetId;

        while (true)
        {
            var info = visited[current];
            var member = memberById[current];
            path.Add((member, info.Relation));
            if (info.Previous == null)
                break;
            current = info.Previous.Value;
        }

        path.Reverse();
        return path;
    }
}
