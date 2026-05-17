using System.Text.Json.Serialization;

namespace FamilyTreeApp.Models;

/// <summary>与后端 FamilyTreeDto 对应的 JSON 模型（camelCase）。</summary>
public class FamilyTreeApiItemDto
{
    [JsonPropertyName("treeId")]
    public ulong TreeId { get; set; }

    [JsonPropertyName("treeName")]
    public string TreeName { get; set; } = "";

    [JsonPropertyName("surname")]
    public string Surname { get; set; } = "";

    [JsonPropertyName("createdByUserId")]
    public ulong CreatedByUserId { get; set; }

    [JsonPropertyName("revisionAt")]
    public DateTime RevisionAt { get; set; }
}

public class FamilyTreeQueryResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("trees")]
    public List<FamilyTreeApiItemDto> Trees { get; set; } = new();

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

public class UserFamilyTreesResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("ownedTrees")]
    public List<FamilyTreeApiItemDto> OwnedTrees { get; set; } = new();

    [JsonPropertyName("managedTrees")]
    public List<FamilyTreeApiItemDto> ManagedTrees { get; set; } = new();

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

public class FamilyTreeCreateResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("treeId")]
    public ulong TreeId { get; set; }

    [JsonPropertyName("treeName")]
    public string TreeName { get; set; } = "";

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

public class FamilyTreeInviteResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

/// <summary>成员增删改等通用 API 响应（避免 Dictionary 反序列化 JsonElement 强转失败）。</summary>
public class ApiActionResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("memberId")]
    public ulong? MemberId { get; set; }
}

public class MemberDto
{
    [JsonPropertyName("memberId")]
    public ulong MemberId { get; set; }

    [JsonPropertyName("treeId")]
    public ulong TreeId { get; set; }

    [JsonPropertyName("fullName")]
    public string FullName { get; set; } = "";

    [JsonPropertyName("gender")]
    public string Gender { get; set; } = "";

    [JsonPropertyName("birthDate")]
    public DateTime? BirthDate { get; set; }

    [JsonPropertyName("deathDate")]
    public DateTime? DeathDate { get; set; }

    [JsonPropertyName("biography")]
    public string? Biography { get; set; }

    [JsonPropertyName("fatherMemberId")]
    public ulong? FatherMemberId { get; set; }

    [JsonPropertyName("motherMemberId")]
    public ulong? MotherMemberId { get; set; }

    [JsonPropertyName("generation")]
    public uint? Generation { get; set; }
}

public class MemberCreateRequest
{
    [JsonPropertyName("treeId")]
    public ulong TreeId { get; set; }

    [JsonPropertyName("fullName")]
    public string FullName { get; set; } = "";

    [JsonPropertyName("gender")]
    public string Gender { get; set; } = "";

    [JsonPropertyName("birthDate")]
    public DateTime? BirthDate { get; set; }

    [JsonPropertyName("deathDate")]
    public DateTime? DeathDate { get; set; }

    [JsonPropertyName("biography")]
    public string? Biography { get; set; }

    [JsonPropertyName("fatherMemberId")]
    public ulong? FatherMemberId { get; set; }

    [JsonPropertyName("motherMemberId")]
    public ulong? MotherMemberId { get; set; }

    [JsonPropertyName("generation")]
    public uint? Generation { get; set; }
}

public class MemberUpdateRequest
{
    [JsonPropertyName("fullName")]
    public string FullName { get; set; } = "";

    [JsonPropertyName("gender")]
    public string Gender { get; set; } = "";

    [JsonPropertyName("birthDate")]
    public DateTime? BirthDate { get; set; }

    [JsonPropertyName("deathDate")]
    public DateTime? DeathDate { get; set; }

    [JsonPropertyName("biography")]
    public string? Biography { get; set; }

    [JsonPropertyName("fatherMemberId")]
    public ulong? FatherMemberId { get; set; }

    [JsonPropertyName("motherMemberId")]
    public ulong? MotherMemberId { get; set; }

    [JsonPropertyName("generation")]
    public uint? Generation { get; set; }
}

public class MemberListResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("total")]
    public int Total { get; set; }

    [JsonPropertyName("page")]
    public int Page { get; set; }

    [JsonPropertyName("pageSize")]
    public int PageSize { get; set; }

    [JsonPropertyName("keyword")]
    public string? Keyword { get; set; }

    [JsonPropertyName("items")]
    public List<MemberDto> Items { get; set; } = new();

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

public class MemberTreeNodeSummaryDto
{
    [JsonPropertyName("memberId")]
    public ulong MemberId { get; set; }

    [JsonPropertyName("fullName")]
    public string FullName { get; set; } = "";

    [JsonPropertyName("gender")]
    public string Gender { get; set; } = "";

    [JsonPropertyName("generation")]
    public uint? Generation { get; set; }

    [JsonPropertyName("relation")]
    public string Relation { get; set; } = "";

    [JsonPropertyName("hasMore")]
    public bool HasMore { get; set; }
}

public class MemberTreeNodesResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("items")]
    public List<MemberTreeNodeSummaryDto> Items { get; set; } = new();

    [JsonPropertyName("hint")]
    public string? Hint { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

public class MemberSingleResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("data")]
    public MemberDto? Data { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

public class MemberTreeNodeDto
{
    [JsonPropertyName("memberId")]
    public ulong MemberId { get; set; }

    [JsonPropertyName("treeId")]
    public ulong TreeId { get; set; }

    [JsonPropertyName("fullName")]
    public string FullName { get; set; } = "";

    [JsonPropertyName("gender")]
    public string Gender { get; set; } = "";

    [JsonPropertyName("birthDate")]
    public DateTime? BirthDate { get; set; }

    [JsonPropertyName("deathDate")]
    public DateTime? DeathDate { get; set; }

    [JsonPropertyName("biography")]
    public string? Biography { get; set; }

    [JsonPropertyName("fatherMemberId")]
    public ulong? FatherMemberId { get; set; }

    [JsonPropertyName("motherMemberId")]
    public ulong? MotherMemberId { get; set; }

    [JsonPropertyName("generation")]
    public uint? Generation { get; set; }

    [JsonPropertyName("relation")]
    public string Relation { get; set; } = "";

    [JsonPropertyName("children")]
    public List<MemberTreeNodeDto> Children { get; set; } = new();
}

public class MemberTreeResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("data")]
    public MemberTreeNodeDto? Data { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("loadedNodeCount")]
    public int LoadedNodeCount { get; set; }

    [JsonPropertyName("maxDepthApplied")]
    public int MaxDepthApplied { get; set; }

    [JsonPropertyName("truncated")]
    public bool Truncated { get; set; }

    [JsonPropertyName("hint")]
    public string? Hint { get; set; }
}

public class MemberRelationNodeDto
{
    [JsonPropertyName("memberId")]
    public ulong MemberId { get; set; }

    [JsonPropertyName("fullName")]
    public string FullName { get; set; } = "";

    [JsonPropertyName("gender")]
    public string Gender { get; set; } = "";

    [JsonPropertyName("generation")]
    public uint? Generation { get; set; }

    [JsonPropertyName("relationToPrevious")]
    public string RelationToPrevious { get; set; } = "";
}

public class MemberRelationshipResponseDto
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("path")]
    public List<MemberRelationNodeDto> Path { get; set; } = new();

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}
