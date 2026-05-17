using System.ComponentModel.DataAnnotations;

namespace FamilyTreeApi.Models;

public class MemberDto
{
    public ulong MemberId { get; set; }
    public ulong TreeId { get; set; }
    public string FullName { get; set; } = "";
    public string Gender { get; set; } = "";
    public DateTime? BirthDate { get; set; }
    public DateTime? DeathDate { get; set; }
    public string? Biography { get; set; }
    public ulong? FatherMemberId { get; set; }
    public ulong? MotherMemberId { get; set; }
    public uint? Generation { get; set; }
}

public class MemberCreateRequest
{
    [Required]
    public ulong TreeId { get; set; }

    [Required, MaxLength(64)]
    public string FullName { get; set; } = "";

    [Required, RegularExpression("^[MF]$")]
    public string Gender { get; set; } = "";

    public DateTime? BirthDate { get; set; }
    public DateTime? DeathDate { get; set; }
    public string? Biography { get; set; }
    public ulong? FatherMemberId { get; set; }
    public ulong? MotherMemberId { get; set; }
    public uint? Generation { get; set; }
}

public class MemberUpdateRequest
{
    [Required, MaxLength(64)]
    public string FullName { get; set; } = "";

    [Required, RegularExpression("^[MF]$")]
    public string Gender { get; set; } = "";

    public DateTime? BirthDate { get; set; }
    public DateTime? DeathDate { get; set; }
    public string? Biography { get; set; }
    public ulong? FatherMemberId { get; set; }
    public ulong? MotherMemberId { get; set; }
    public uint? Generation { get; set; }
}

public class MemberListQuery
{
    [Required]
    public ulong TreeId { get; set; }

    [Range(1, int.MaxValue)]
    public int Page { get; set; } = 1;

    [Range(1, 10000)]
    public int PageSize { get; set; } = 50;

    /// <summary>按姓名模糊筛选（可选）</summary>
    public string? Keyword { get; set; }
}

/// <summary>成员姓名模糊查询（关键字留空则返回该族谱全部成员）</summary>
public class MemberSearchQuery
{
    [Required]
    public ulong TreeId { get; set; }

    [MaxLength(64)]
    public string? Keyword { get; set; }

    [Range(1, int.MaxValue)]
    public int Page { get; set; } = 1;

    [Range(1, 10000)]
    public int PageSize { get; set; } = 500;
}

public class MemberListResponse
{
    public bool Success { get; set; } = true;
    public int Total { get; set; }
    public int Page { get; set; }
    public int PageSize { get; set; }
    public string? Keyword { get; set; }
    public List<MemberDto> Items { get; set; } = new();
    public string? Message { get; set; }
}

public class MemberTreeNodeDto
{
    public ulong MemberId { get; set; }
    public ulong TreeId { get; set; }
    public string FullName { get; set; } = "";
    public string Gender { get; set; } = "";
    public DateTime? BirthDate { get; set; }
    public DateTime? DeathDate { get; set; }
    public string? Biography { get; set; }
    public ulong? FatherMemberId { get; set; }
    public ulong? MotherMemberId { get; set; }
    public uint? Generation { get; set; }
    public string Relation { get; set; } = "";
    public List<MemberTreeNodeDto> Children { get; set; } = new();
}

public class MemberTreeNodeSummaryDto
{
    public ulong MemberId { get; set; }
    public string FullName { get; set; } = "";
    public string Gender { get; set; } = "";
    public uint? Generation { get; set; }
    public string Relation { get; set; } = "";
    public bool HasMore { get; set; }
}

public class MemberTreeNodesResponse
{
    public bool Success { get; set; }
    public List<MemberTreeNodeSummaryDto> Items { get; set; } = new();
    public string? Hint { get; set; }
    public string? Message { get; set; }
}

public class MemberTreeResponse
{
    public bool Success { get; set; }
    public MemberTreeNodeDto? Data { get; set; }
    public string? Message { get; set; }
    public int LoadedNodeCount { get; set; }
    public int MaxDepthApplied { get; set; }
    public bool Truncated { get; set; }
    public string? Hint { get; set; }
}

public class MemberRelationNodeDto
{
    public ulong MemberId { get; set; }
    public string FullName { get; set; } = "";
    public string Gender { get; set; } = "";
    public uint? Generation { get; set; }
    public string RelationToPrevious { get; set; } = "";
}

public class MemberRelationshipResponse
{
    public bool Success { get; set; }
    public List<MemberRelationNodeDto> Path { get; set; } = new();
    public string? Message { get; set; }
}
