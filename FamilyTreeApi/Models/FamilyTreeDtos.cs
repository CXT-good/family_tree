namespace FamilyTreeApi.Models;

public class FamilyTreeQueryRequest
{
    public string? Keyword { get; set; }
}

public class FamilyTreeCreateRequest
{
    public required string TreeName { get; set; }
}

public class FamilyTreeDto
{
    public ulong TreeId { get; set; }
    public string TreeName { get; set; } = "";
    public string Surname { get; set; } = "";
    public ulong CreatedByUserId { get; set; }
    public DateTime RevisionAt { get; set; }
}

public class UserFamilyTreesResponse
{
    public bool Success { get; set; } = true;
    public List<FamilyTreeDto> OwnedTrees { get; set; } = new();
    public List<FamilyTreeDto> ManagedTrees { get; set; } = new();
}

public class FamilyTreeQueryResponse
{
    public bool Success { get; set; } = true;
    public List<FamilyTreeDto> Trees { get; set; } = new();
    public string? Message { get; set; }
}

public class FamilyTreeCreateResponse
{
    public bool Success { get; set; } = true;
    public ulong TreeId { get; set; }
    public string TreeName { get; set; } = "";
    public string? Message { get; set; }
}

public class FamilyTreeInviteRequest
{
    public ulong InviteeUserId { get; set; }
}

public class FamilyTreeInviteResponse
{
    public bool Success { get; set; } = true;
    public string? Message { get; set; }
}
