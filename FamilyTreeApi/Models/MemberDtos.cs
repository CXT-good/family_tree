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

    [Range(1, 500)]
    public int PageSize { get; set; } = 50;

    /// <summary>按姓名模糊筛选（可选）</summary>
    public string? Keyword { get; set; }
}
