namespace FamilyTreeApi.Models;

public class MemberQueryRowDto
{
    public ulong MemberId { get; set; }
    public string FullName { get; set; } = "";
    public string Gender { get; set; } = "";
    public uint? Generation { get; set; }
    public DateTime? BirthDate { get; set; }
    public DateTime? DeathDate { get; set; }
    public string? RelationKind { get; set; }
    public int? Depth { get; set; }
    public int? AgeYears { get; set; }
    public int? BirthYear { get; set; }
    public double? GenerationAvgBirthYear { get; set; }
    public double? AvgLifespanYears { get; set; }
    public int? MemberCount { get; set; }
}

public class MemberAdvancedQueryResponse
{
    public bool Success { get; set; }
    public string? Message { get; set; }
    public string? Summary { get; set; }
    public int Total { get; set; }
    public int Page { get; set; }
    public int PageSize { get; set; }
    public List<MemberQueryRowDto> Rows { get; set; } = new();
}
