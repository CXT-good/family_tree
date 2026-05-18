using System.Net.Http;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Interop;
using System.Windows.Media;
using System.Windows.Threading;
using FamilyTreeApp.Controls;
using FamilyTreeApp.Helpers;
using FamilyTreeApp.Models;

namespace FamilyTreeApp.Pages;

public partial class ClanManagePage : Page
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        MaxDepth = 128
    };

    private readonly HttpClient _httpClient;
    private readonly ulong _userId;
    private List<ClanInfo> _createdClans = new();
    private List<ClanInfo> _managedClans = new();
    private ulong _currentTreeId;

    private const int MemberSearchPageSize = 80;
    private const int SqlQueryPageSize = 40;
    private ulong _memberSearchTreeId;
    private string? _memberSearchKeyword;
    private int _memberSearchPage = 1;
    private int _memberSearchTotal;

    private string? _sqlQueryApiPath;
    private ulong _sqlQueryTreeId;
    private ulong _sqlQueryMemberId;
    private int _sqlQueryPage = 1;
    private int _sqlQueryTotal;
    private bool _sqlQueryUsesPaging = true;

    public ClanManagePage(ulong userId)
    {
        InitializeComponent();
        _userId = userId;
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri("http://localhost:5000/"),
            Timeout = TimeSpan.FromMinutes(3)
        };
        Loaded += async (_, _) => await LoadUserFamilyTreesAsync();
    }

    private static ClanInfo MapTreeDtoToClanInfo(FamilyTreeApiItemDto dto)
    {
        var revisionLocal = dto.RevisionAt.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(dto.RevisionAt, DateTimeKind.Local)
            : dto.RevisionAt.ToLocalTime();
        var revisionText = revisionLocal.ToString("yyyy-MM-dd HH:mm");

        return new ClanInfo(
            id: dto.TreeId.ToString(),
            name: dto.TreeName,
            surname: dto.Surname,
            creator: dto.CreatedByUserId.ToString(),
            lastModified: revisionText,
            totalMembers: 0,
            maleCount: 0,
            femaleCount: 0,
            createDate: revisionText);
    }

    /// <summary>根据当前登录用户从 tree_managers 加载「我创建的」(owner) 与「我管理的」(editor)。</summary>
    private async Task LoadUserFamilyTreesAsync()
    {
        try
        {
            var response = await _httpClient.GetAsync($"api/familytrees/user?userId={_userId}");
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<UserFamilyTreesResponseDto>(body, JsonOptions);

            if (result is not { Success: true })
            {
                MessageBox.Show(result?.Message ?? $"加载失败（HTTP {(int)response.StatusCode}）", "族谱",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            _createdClans = result.OwnedTrees.Select(MapTreeDtoToClanInfo).ToList();
            _managedClans = result.ManagedTrees.Select(MapTreeDtoToClanInfo).ToList();
            RefreshDisplay();
        }
        catch (Exception ex)
        {
            MessageBox.Show($"加载族谱数据失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void RefreshDisplay()
    {
        MyCreatedClansList.ItemsSource = null;
        MyCreatedClansList.ItemsSource = _createdClans;

        MyManagedClansList.ItemsSource = null;
        MyManagedClansList.ItemsSource = _managedClans;
    }

    private async void QueryButton_Click(object sender, RoutedEventArgs e)
    {
        var keyword = QueryKeywordInput.Text?.Trim() ?? "";
        if (string.IsNullOrEmpty(keyword))
        {
            MessageBox.Show("请输入谱名、姓氏、族谱ID 或 创建者ID 后再查询。", "查询", MessageBoxButton.OK,
                MessageBoxImage.Information);
            return;
        }

        try
        {
            var response = await _httpClient.PostAsJsonAsync("api/familytrees/search", new { keyword });
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<FamilyTreeQueryResponseDto>(body, JsonOptions);

            if (result is not { Success: true })
            {
                MessageBox.Show(result?.Message ?? "查询失败", "查询", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            var rows = result.Trees.Select(MapTreeDtoToClanInfo).ToList();
            QueryResultsList.ItemsSource = rows;

            if (rows.Count == 0)
                MessageBox.Show("没有符合条件的族谱。", "查询", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"查询失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    /// <summary>创建族谱：当前登录用户为创建者，姓氏为族谱名首字，族谱ID 与修谱时间由数据库/服务器生成。</summary>
    private async void CreateClanConfirmButton_Click(object sender, RoutedEventArgs e)
    {
        var name = CreateTreeNameInput.Text?.Trim() ?? "";
        if (string.IsNullOrEmpty(name))
        {
            MessageBox.Show("请输入族谱名称。", "创建族谱", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        try
        {
            var response = await _httpClient.PostAsJsonAsync($"api/familytrees/create?userId={_userId}",
                new { treeName = name });
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<FamilyTreeCreateResponseDto>(body, JsonOptions);

            if (result is { Success: true })
            {
                MessageBox.Show(
                    string.IsNullOrEmpty(result.Message)
                        ? $"创建成功。族谱ID: {result.TreeId}"
                        : $"{result.Message} 族谱ID: {result.TreeId}",
                    "创建族谱", MessageBoxButton.OK, MessageBoxImage.Information);
                CreateTreeNameInput.Text = "";
                await LoadUserFamilyTreesAsync();
            }
            else
            {
                MessageBox.Show(result?.Message ?? $"创建失败（HTTP {(int)response.StatusCode}）", "创建族谱",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"创建失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private async void BranchPreviewButton_Click(object sender, RoutedEventArgs e)
    {
        var treeIdText = ((PlaceholderTextBox)TreeIdInput).GetActualText().Trim();
        var rootIdText = ((PlaceholderTextBox)BranchRootIdInput).GetActualText().Trim();
        if (!ulong.TryParse(treeIdText, out var treeId) || treeId == 0)
        {
            MessageBox.Show("请输入有效的族谱ID。", "分支预览", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        if (!ulong.TryParse(rootIdText, out var rootMemberId) || rootMemberId == 0)
        {
            MessageBox.Show("请输入有效的根成员ID。", "分支预览", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var button = (Button)sender;
        try
        {
            button.IsEnabled = false;
            Mouse.OverrideCursor = Cursors.Wait;
            await InitLazyBranchTreeAsync(treeId, rootMemberId);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"分支预览失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            Mouse.OverrideCursor = null;
            button.IsEnabled = true;
        }
    }

    private async void AncestorQueryButton_Click(object sender, RoutedEventArgs e)
    {
        var treeIdText = ((PlaceholderTextBox)TreeIdInput).GetActualText().Trim();
        var memberIdText = ((PlaceholderTextBox)AncestorMemberIdInput).GetActualText().Trim();

        if (!ulong.TryParse(treeIdText, out var treeId) || treeId == 0)
        {
            MessageBox.Show("请输入有效的族谱ID。", "祖先查询", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        if (!ulong.TryParse(memberIdText, out var memberId) || memberId == 0)
        {
            MessageBox.Show("请输入有效的成员ID。", "祖先查询", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var button = (Button)sender;
        try
        {
            button.IsEnabled = false;
            Mouse.OverrideCursor = Cursors.Wait;
            await InitLazyAncestorTreeAsync(treeId, memberId);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"祖先查询失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            Mouse.OverrideCursor = null;
            button.IsEnabled = true;
        }
    }

    private async void RelationshipQueryButton_Click(object sender, RoutedEventArgs e)
    {
        var treeIdText = ((PlaceholderTextBox)TreeIdInput).GetActualText().Trim();
        var memberA = ((PlaceholderTextBox)RelationMemberIdAInput).GetActualText().Trim();
        var memberB = ((PlaceholderTextBox)RelationMemberIdBInput).GetActualText().Trim();

        if (!ulong.TryParse(treeIdText, out var treeId) || treeId == 0)
        {
            MessageBox.Show("请输入有效的族谱ID。", "亲缘关系查询", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        if (!ulong.TryParse(memberA, out var memberIdA) || memberIdA == 0 || !ulong.TryParse(memberB, out var memberIdB) || memberIdB == 0)
        {
            MessageBox.Show("请输入两个有效的成员ID。", "亲缘关系查询", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        try
        {
            RelationPathList.ItemsSource = null;
            var response = await _httpClient.GetAsync($"api/Members/relationship?treeId={treeId}&memberId1={memberIdA}&memberId2={memberIdB}");
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<MemberRelationshipResponseDto>(body, JsonOptions);

            if (result is not { Success: true, Path: { Count: > 0 } path })
            {
                MessageBox.Show(result?.Message ?? "未找到亲缘关系通路。", "亲缘关系查询", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            RelationPathList.ItemsSource = path;
        }
        catch (Exception ex)
        {
            MessageBox.Show($"亲缘关系查询失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void PageRoot_Loaded(object sender, RoutedEventArgs e)
    {
        ScrollViewerHelper.ApplySmoothScrolling(MainPageScroll);
        ScrollViewerHelper.ApplyToDescendants(PageRoot);
    }

    private async void SqlSpouseChildren_Click(object sender, RoutedEventArgs e) =>
        await RunSqlQueryAsync(sender, "配偶与子女", requireMemberId: true, "spouse-and-children", usesPaging: true);

    private async void SqlLongestLifespanGen_Click(object sender, RoutedEventArgs e) =>
        await RunSqlQueryAsync(sender, "最长寿辈分", requireMemberId: false, "longest-lifespan-generation", usesPaging: false);

    private async void SqlMalesOver50NoSpouse_Click(object sender, RoutedEventArgs e) =>
        await RunSqlQueryAsync(sender, "50岁无配偶男性", requireMemberId: false, "males-over-50-no-spouse", usesPaging: true);

    private async void SqlEarlierThanGenAvgBirth_Click(object sender, RoutedEventArgs e) =>
        await RunSqlQueryAsync(sender, "早于辈分均年出生", requireMemberId: false, "earlier-than-generation-avg-birth", usesPaging: true);

    private async void SqlQueryPrevPageButton_Click(object sender, RoutedEventArgs e)
    {
        if (_sqlQueryPage <= 1 || string.IsNullOrEmpty(_sqlQueryApiPath)) return;
        _sqlQueryPage--;
        await LoadSqlQueryPageAsync("SQL 查询");
    }

    private async void SqlQueryNextPageButton_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrEmpty(_sqlQueryApiPath)) return;
        var totalPages = _sqlQueryTotal == 0 ? 0 : (int)Math.Ceiling(_sqlQueryTotal / (double)SqlQueryPageSize);
        if (_sqlQueryPage >= totalPages) return;
        _sqlQueryPage++;
        await LoadSqlQueryPageAsync("SQL 查询");
    }

    private async Task RunSqlQueryAsync(
        object sender,
        string title,
        bool requireMemberId,
        string apiPath,
        bool usesPaging)
    {
        ulong memberId = 0;
        if (requireMemberId)
        {
            var memberIdText = SqlQueryMemberIdInput.GetActualText().Trim();
            if (!ulong.TryParse(memberIdText, out memberId) || memberId == 0)
            {
                MessageBox.Show("请输入有效的成员 ID（「配偶与子女」查询需要）。", title, MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }
        }

        var treeId = await ResolveSqlQueryTreeIdAsync(memberId);
        if (treeId == 0)
        {
            MessageBox.Show(
                "请输入有效的族谱 ID（本区「族谱ID」框）。\n\n若只填了成员 ID，请确认该成员在库中存在；或先在族谱列表中点击「成员管理」选中族谱。",
                title,
                MessageBoxButton.OK,
                MessageBoxImage.Information);
            return;
        }

        _sqlQueryApiPath = apiPath;
        _sqlQueryTreeId = treeId;
        _sqlQueryMemberId = memberId;
        _sqlQueryUsesPaging = usesPaging;
        _sqlQueryPage = 1;

        var button = sender as Button;
        try
        {
            if (button is not null) button.IsEnabled = false;
            await LoadSqlQueryPageAsync(title);
        }
        finally
        {
            if (button is not null) button.IsEnabled = true;
        }
    }

    private async Task LoadSqlQueryPageAsync(string title)
    {
        if (string.IsNullOrEmpty(_sqlQueryApiPath)) return;

        try
        {
            Mouse.OverrideCursor = Cursors.Wait;
            SqlQuerySummary.Text = "查询中…";

            var url = BuildSqlQueryUrl();
            var response = await _httpClient.GetAsync(url);
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<MemberAdvancedQueryResponseDto>(body, JsonOptions);

            if (result is not { Success: true })
            {
                SqlQuerySummary.Text = "";
                SqlQueryResultList.ItemsSource = null;
                SqlQueryPagingPanel.Visibility = Visibility.Collapsed;
                MessageBox.Show(result?.Message ?? "查询失败。", title, MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            _sqlQueryTotal = result.Total;
            _sqlQueryPage = result.Page > 0 ? result.Page : _sqlQueryPage;
            SqlQuerySummary.Text = result.Summary ?? $"返回 {result.Rows.Count} 条。";
            SqlQueryResultList.ItemsSource = result.Rows.Select(FormatSqlQueryRow).ToList();
            UpdateSqlQueryPagingUi(result);
        }
        catch (Exception ex)
        {
            SqlQuerySummary.Text = "";
            MessageBox.Show($"{title}失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            Mouse.OverrideCursor = null;
        }
    }

    private string BuildSqlQueryUrl()
    {
        var url = $"api/MemberQueries/{_sqlQueryApiPath}?treeId={_sqlQueryTreeId}";
        if (_sqlQueryMemberId > 0)
            url += $"&memberId={_sqlQueryMemberId}";
        if (_sqlQueryUsesPaging)
        {
            url += $"&page={_sqlQueryPage}&pageSize={SqlQueryPageSize}";
            if (_sqlQueryPage > 1 && _sqlQueryTotal > 0)
                url += $"&skipCount=true&knownTotal={_sqlQueryTotal}";
        }

        return url;
    }

    private void UpdateSqlQueryPagingUi(MemberAdvancedQueryResponseDto result)
    {
        if (!_sqlQueryUsesPaging || result.Total <= SqlQueryPageSize)
        {
            SqlQueryPagingPanel.Visibility = Visibility.Collapsed;
            return;
        }

        var pageSize = result.PageSize > 0 ? result.PageSize : SqlQueryPageSize;
        var totalPages = (int)Math.Ceiling(result.Total / (double)pageSize);
        SqlQueryPagingPanel.Visibility = Visibility.Visible;
        SqlQueryPageInfo.Text = $"第 {result.Page} / {totalPages} 页（共 {result.Total} 条，每页 {pageSize} 条）";
        SqlQueryPrevPageButton.IsEnabled = result.Page > 1;
        SqlQueryNextPageButton.IsEnabled = result.Page < totalPages;
    }

    private static string FormatSqlQueryRow(MemberQueryRowDto r)
    {
        if (r.RelationKind == "统计")
        {
            return $"{r.FullName} · 平均寿命约 {r.AvgLifespanYears:F2} 年 · 样本 {r.MemberCount} 人";
        }

        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(r.RelationKind))
            parts.Add($"[{r.RelationKind}]");
        if (r.MemberId > 0)
            parts.Add($"ID {r.MemberId}");
        parts.Add(r.FullName);
        if (!string.IsNullOrWhiteSpace(r.Gender))
            parts.Add(r.Gender);
        if (r.Generation.HasValue)
            parts.Add($"第{r.Generation}代");
        if (r.Depth.HasValue)
            parts.Add($"上{r.Depth}代");
        if (r.AgeYears.HasValue)
            parts.Add($"{r.AgeYears}岁");
        if (r.BirthYear.HasValue)
            parts.Add($"生于{r.BirthYear}");
        if (r.GenerationAvgBirthYear.HasValue)
            parts.Add($"辈分均年{ r.GenerationAvgBirthYear:F1}");
        if (r.BirthDate.HasValue)
            parts.Add($"生日 {r.BirthDate:yyyy-MM-dd}");
        return string.Join(" · ", parts);
    }

    private async void MemberSearchButton_Click(object sender, RoutedEventArgs e)
    {
        var keyword = MemberSearchNameInput.GetActualText().Trim();
        _memberSearchPage = 1;
        await RunMemberSearchAsync(sender, string.IsNullOrEmpty(keyword) ? null : keyword, showEmptyDialog: true);
    }

    private async void MemberShowAllButton_Click(object sender, RoutedEventArgs e)
    {
        _memberSearchPage = 1;
        await RunMemberSearchAsync(sender, keyword: null, showEmptyDialog: true);
    }

    private async void MemberSearchPrevPageButton_Click(object sender, RoutedEventArgs e)
    {
        if (_memberSearchPage <= 1) return;
        _memberSearchPage--;
        await LoadMemberSearchPageAsync(sender);
    }

    private async void MemberSearchNextPageButton_Click(object sender, RoutedEventArgs e)
    {
        var totalPages = GetMemberSearchTotalPages();
        if (_memberSearchPage >= totalPages) return;
        _memberSearchPage++;
        await LoadMemberSearchPageAsync(sender);
    }

    private int GetMemberSearchTotalPages() =>
        _memberSearchTotal <= 0 ? 1 : (int)Math.Ceiling(_memberSearchTotal / (double)MemberSearchPageSize);

    private async Task RunMemberSearchAsync(object sender, string? keyword, bool showEmptyDialog)
    {
        var treeIdText = MemberSearchTreeIdInput.GetActualText().Trim();
        if (!ulong.TryParse(treeIdText, out var treeId) || treeId == 0)
        {
            MessageBox.Show("请输入有效的族谱 ID。", "成员查找", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        _memberSearchTreeId = treeId;
        _memberSearchKeyword = keyword;
        await LoadMemberSearchPageAsync(sender, showEmptyDialog);
    }

    private async Task LoadMemberSearchPageAsync(object? triggerButton, bool showEmptyDialog = false)
    {
        var buttons = new List<Button>();
        if (triggerButton is Button b) buttons.Add(b);
        if (MemberSearchPrevPageButton != triggerButton) buttons.Add(MemberSearchPrevPageButton);
        if (MemberSearchNextPageButton != triggerButton) buttons.Add(MemberSearchNextPageButton);

        try
        {
            foreach (var btn in buttons) btn.IsEnabled = false;
            MemberSearchResultsList.ItemsSource = null;
            MemberSearchSummary.Text = "正在加载…";
            Mouse.OverrideCursor = Cursors.Wait;

            var result = await FetchMemberSearchPageAsync(_memberSearchTreeId, _memberSearchKeyword, _memberSearchPage);
            _memberSearchTotal = result.Total;

            var totalPages = GetMemberSearchTotalPages();
            if (_memberSearchPage > totalPages)
                _memberSearchPage = totalPages;
            if (_memberSearchPage < 1)
                _memberSearchPage = 1;

            MemberSearchResultsList.ItemsSource = result.Items;
            UpdateMemberSearchPaginationUi();

            if (_memberSearchKeyword is null)
            {
                MemberSearchSummary.Text = _memberSearchTotal == 0
                    ? $"族谱 {_memberSearchTreeId} 中暂无成员"
                    : $"共 {_memberSearchTotal:N0} 人 · 当前第 {_memberSearchPage} / {totalPages} 页（每页 {MemberSearchPageSize} 人）";
            }
            else
            {
                MemberSearchSummary.Text = _memberSearchTotal == 0
                    ? $"未找到姓名包含「{_memberSearchKeyword}」的成员"
                    : $"匹配 {_memberSearchTotal:N0} 人 · 关键字「{_memberSearchKeyword}」· 第 {_memberSearchPage} / {totalPages} 页";
            }

            if (showEmptyDialog && result.Items.Count == 0)
                MessageBox.Show(MemberSearchSummary.Text, "成员查找", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MemberSearchSummary.Text = "加载失败";
            MessageBox.Show(ex.Message, "成员查找", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            Mouse.OverrideCursor = null;
            foreach (var btn in buttons) btn.IsEnabled = true;
            UpdateMemberSearchPaginationUi();
        }
    }

    private void UpdateMemberSearchPaginationUi()
    {
        var totalPages = GetMemberSearchTotalPages();
        MemberSearchPageInfo.Text = _memberSearchTotal <= 0
            ? "无数据"
            : $"第 {_memberSearchPage} / {totalPages} 页";
        MemberSearchPrevPageButton.IsEnabled = _memberSearchPage > 1;
        MemberSearchNextPageButton.IsEnabled = _memberSearchPage < totalPages && _memberSearchTotal > 0;
    }

    private async Task<MemberListResponseDto> FetchMemberSearchPageAsync(ulong treeId, string? keyword, int page)
    {
        var url = string.IsNullOrEmpty(keyword)
            ? $"api/Members?treeId={treeId}&page={page}&pageSize={MemberSearchPageSize}"
            : $"api/Members/search?treeId={treeId}&keyword={Uri.EscapeDataString(keyword)}&page={page}&pageSize={MemberSearchPageSize}";

        var response = await _httpClient.GetAsync(url).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException(ParseApiErrorMessage(body, response.StatusCode, "加载成员失败"));

        var result = JsonSerializer.Deserialize<MemberListResponseDto>(body, JsonOptions);
        if (result is not { Success: true })
            throw new InvalidOperationException(result?.Message ?? "加载成员列表失败");

        result.Items ??= new List<MemberDto>();
        return result;
    }

    private async Task<List<MemberDto>> LoadMembersForTreeAsync(ulong treeId)
    {
        if (treeId == 0)
            throw new InvalidOperationException("未选择族谱，请先点击「操作成员」。");

        var response = await _httpClient.GetAsync($"api/Members?treeId={treeId}&page=1&pageSize=500");
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException(ParseApiErrorMessage(body, response.StatusCode, "加载成员失败"));

        MemberListResponseDto? result;
        try
        {
            result = JsonSerializer.Deserialize<MemberListResponseDto>(body, JsonOptions);
        }
        catch (JsonException)
        {
            throw new InvalidOperationException(ParseApiErrorMessage(body, response.StatusCode, "加载成员失败"));
        }

        if (result is not { Success: true })
            throw new InvalidOperationException(result?.Message ?? "加载成员列表失败");

        return result.Items ?? new List<MemberDto>();
    }

    private static string ParseApiErrorMessage(string body, System.Net.HttpStatusCode statusCode, string prefix)
    {
        if (!string.IsNullOrWhiteSpace(body))
        {
            try
            {
                var err = JsonSerializer.Deserialize<ApiActionResponseDto>(body, JsonOptions);
                if (!string.IsNullOrWhiteSpace(err?.Message))
                    return err.Message;
            }
            catch
            {
                // 非 JSON（如旧版 API 返回的 MySql 异常纯文本）
            }

            var trimmed = body.Trim();
            if (trimmed.Length > 200)
                trimmed = trimmed[..200] + "…";
            if (!string.IsNullOrEmpty(trimmed))
                return $"{prefix}（HTTP {(int)statusCode}）：{trimmed}";
        }

        return $"{prefix}（HTTP {(int)statusCode}）";
    }

    private async Task LoadMembersIntoListAsync(ListBox membersList)
    {
        try
        {
            var items = await LoadMembersForTreeAsync(_currentTreeId).ConfigureAwait(false);
            await membersList.Dispatcher.InvokeAsync(() =>
            {
                membersList.ItemsSource = items;
                membersList.DisplayMemberPath = "FullName";
            });
        }
        catch (Exception ex)
        {
            await membersList.Dispatcher.InvokeAsync(() =>
                MessageBox.Show(ex.Message, "错误", MessageBoxButton.OK, MessageBoxImage.Error));
        }
    }

    private static bool TryParseActionResponse(HttpResponseMessage response, string body, out string? errorMessage)
    {
        ApiActionResponseDto? result = null;
        try
        {
            result = JsonSerializer.Deserialize<ApiActionResponseDto>(body, JsonOptions);
        }
        catch
        {
            // 非 JSON 响应
        }

        if (response.IsSuccessStatusCode && (result is null || result.Success))
        {
            errorMessage = null;
            return true;
        }

        errorMessage = result?.Message ?? $"请求失败（HTTP {(int)response.StatusCode}）";
        return false;
    }

    private sealed class LazyTreeNodeTag
    {
        public required ulong TreeId { get; init; }
        public required ulong MemberId { get; init; }
        public required bool IsBranch { get; init; }
        public bool ChildrenLoaded { get; set; }
    }

    private async Task InitLazyBranchTreeAsync(ulong treeId, ulong rootMemberId)
    {
        BranchTreeView.Items.Clear();
        BranchQueryHint.Text = "正在加载根节点…";

        var root = await FetchMemberAsync(treeId, rootMemberId);
        if (root is null)
        {
            BranchQueryHint.Text = "";
            MessageBox.Show("未找到根成员。", "分支预览", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        var children = await FetchChildrenAsync(treeId, rootMemberId);
        var rootSummary = new MemberTreeNodeSummaryDto
        {
            MemberId = root.MemberId,
            FullName = root.FullName,
            Gender = root.Gender,
            Generation = root.Generation,
            Relation = "当前成员",
            HasMore = children.Count > 0
        };

        var rootItem = CreateBranchTreeItem(treeId, rootSummary, childrenLoaded: true);
        foreach (var child in children)
            rootItem.Items.Add(CreateBranchTreeItem(treeId, child));

        BranchTreeView.Items.Add(rootItem);
        rootItem.IsExpanded = true;
        BranchQueryHint.Text =
            $"已显示根节点及 {children.Count} 位直系子女。点击带「▸」的节点可继续展开查看该支全部后代。";
    }

    private async Task InitLazyAncestorTreeAsync(ulong treeId, ulong memberId)
    {
        AncestorTreeView.Items.Clear();
        AncestorQueryHint.Text = "正在加载…";

        var member = await FetchMemberAsync(treeId, memberId);
        if (member is null)
        {
            AncestorQueryHint.Text = "";
            MessageBox.Show("未找到成员。", "祖先查询", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        var parents = await FetchParentsAsync(treeId, memberId);
        var rootSummary = new MemberTreeNodeSummaryDto
        {
            MemberId = member.MemberId,
            FullName = member.FullName,
            Gender = member.Gender,
            Generation = member.Generation,
            Relation = "当前成员",
            HasMore = parents.Count > 0
        };

        var rootItem = CreateAncestorTreeItem(treeId, rootSummary, childrenLoaded: true);
        foreach (var parent in parents)
            rootItem.Items.Add(CreateAncestorTreeItem(treeId, parent));

        AncestorTreeView.Items.Add(rootItem);
        rootItem.IsExpanded = true;
        AncestorQueryHint.Text =
            $"已显示本人及 {parents.Count} 位父母。点击带「▸」的节点可继续向上展开查看全部祖先。";
    }

    private async Task<MemberDto?> FetchMemberAsync(ulong treeId, ulong memberId)
    {
        var url = treeId > 0 ? $"api/Members/{memberId}?treeId={treeId}" : $"api/Members/{memberId}";
        var response = await _httpClient.GetAsync(url);
        var body = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<MemberSingleResponseDto>(body, JsonOptions);
        return result is { Success: true, Data: { } data } ? data : null;
    }

    private static ulong ParsePlaceholderId(PlaceholderTextBox box)
    {
        var text = box.GetActualText().Trim();
        return ulong.TryParse(text, out var id) ? id : 0;
    }

    private static void SetPlaceholderBoxValue(PlaceholderTextBox box, string value)
    {
        box.Focus();
        box.Text = value;
        box.CaretIndex = value.Length;
        Keyboard.ClearFocus();
    }

    private void ApplyCurrentTreeIdToQueryInputs()
    {
        if (_currentTreeId == 0) return;
        var idText = _currentTreeId.ToString();
        SetPlaceholderBoxValue(SqlQueryTreeIdInput, idText);
        SetPlaceholderBoxValue(MemberSearchTreeIdInput, idText);
        SetPlaceholderBoxValue((PlaceholderTextBox)TreeIdInput, idText);
    }

    private async Task<ulong> ResolveSqlQueryTreeIdAsync(ulong memberIdHint)
    {
        var treeId = ParsePlaceholderId(SqlQueryTreeIdInput);
        if (treeId > 0) return treeId;

        treeId = ParsePlaceholderId(MemberSearchTreeIdInput);
        if (treeId > 0) return treeId;

        treeId = ParsePlaceholderId((PlaceholderTextBox)TreeIdInput);
        if (treeId > 0) return treeId;

        if (_currentTreeId > 0) return _currentTreeId;

        if (memberIdHint == 0) return 0;

        var member = await FetchMemberAsync(0, memberIdHint);
        if (member is { TreeId: > 0 })
        {
            SetPlaceholderBoxValue(SqlQueryTreeIdInput, member.TreeId.ToString());
            return member.TreeId;
        }

        return 0;
    }

    private async Task<List<MemberTreeNodeSummaryDto>> FetchChildrenAsync(ulong treeId, ulong memberId)
    {
        var response = await _httpClient.GetAsync($"api/Members/children?treeId={treeId}&memberId={memberId}");
        var body = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<MemberTreeNodesResponseDto>(body, JsonOptions);
        return result is { Success: true } ? result.Items : [];
    }

    private async Task<List<MemberTreeNodeSummaryDto>> FetchParentsAsync(ulong treeId, ulong memberId)
    {
        var response = await _httpClient.GetAsync($"api/Members/parents?treeId={treeId}&memberId={memberId}");
        var body = await response.Content.ReadAsStringAsync();
        var result = JsonSerializer.Deserialize<MemberTreeNodesResponseDto>(body, JsonOptions);
        return result is { Success: true } ? result.Items : [];
    }

    private TreeViewItem CreateBranchTreeItem(ulong treeId, MemberTreeNodeSummaryDto node, bool childrenLoaded = false)
    {
        var item = new TreeViewItem { Header = FormatTreeHeader(node) };
        if (node.HasMore && !childrenLoaded)
        {
            item.Tag = new LazyTreeNodeTag { TreeId = treeId, MemberId = node.MemberId, IsBranch = true };
            item.Items.Add(CreateTreePlaceholderItem());
            item.Expanded += LazyBranchItem_Expanded;
        }

        return item;
    }

    private TreeViewItem CreateAncestorTreeItem(ulong treeId, MemberTreeNodeSummaryDto node, bool childrenLoaded = false)
    {
        var item = new TreeViewItem { Header = FormatTreeHeader(node) };
        if (node.HasMore && !childrenLoaded)
        {
            item.Tag = new LazyTreeNodeTag { TreeId = treeId, MemberId = node.MemberId, IsBranch = false };
            item.Items.Add(CreateTreePlaceholderItem());
            item.Expanded += LazyAncestorItem_Expanded;
        }

        return item;
    }

    private static TreeViewItem CreateTreePlaceholderItem() =>
        new() { Header = "▸ 点击展开", IsEnabled = false };

    private static string FormatTreeHeader(MemberTreeNodeSummaryDto node)
    {
        var header = string.IsNullOrWhiteSpace(node.Relation)
            ? node.FullName
            : $"{node.Relation}: {node.FullName}";
        if (node.Generation.HasValue)
            header += $" · 世代 {node.Generation}";
        if (!string.IsNullOrWhiteSpace(node.Gender))
            header += $" · {node.Gender}";
        header += $" (ID: {node.MemberId})";
        if (node.HasMore)
            header += " ▸";
        return header;
    }

    private async void LazyBranchItem_Expanded(object sender, RoutedEventArgs e)
    {
        if (sender is not TreeViewItem item || item.Tag is not LazyTreeNodeTag tag || tag.ChildrenLoaded)
            return;

        tag.ChildrenLoaded = true;
        item.Expanded -= LazyBranchItem_Expanded;
        item.Items.Clear();
        item.Items.Add(new TreeViewItem { Header = "加载中…", IsEnabled = false });

        try
        {
            var children = await FetchChildrenAsync(tag.TreeId, tag.MemberId);
            item.Items.Clear();
            if (children.Count == 0)
            {
                item.Items.Add(new TreeViewItem { Header = "（无子女）", IsEnabled = false });
                return;
            }

            foreach (var child in children)
                item.Items.Add(CreateBranchTreeItem(tag.TreeId, child));
        }
        catch (Exception ex)
        {
            item.Items.Clear();
            item.Items.Add(new TreeViewItem { Header = $"加载失败: {ex.Message}", IsEnabled = false });
            tag.ChildrenLoaded = false;
            item.Expanded += LazyBranchItem_Expanded;
        }
    }

    private async void LazyAncestorItem_Expanded(object sender, RoutedEventArgs e)
    {
        if (sender is not TreeViewItem item || item.Tag is not LazyTreeNodeTag tag || tag.ChildrenLoaded)
            return;

        tag.ChildrenLoaded = true;
        item.Expanded -= LazyAncestorItem_Expanded;
        item.Items.Clear();
        item.Items.Add(new TreeViewItem { Header = "加载中…", IsEnabled = false });

        try
        {
            var parents = await FetchParentsAsync(tag.TreeId, tag.MemberId);
            item.Items.Clear();
            if (parents.Count == 0)
            {
                item.Items.Add(new TreeViewItem { Header = "（无父母记录）", IsEnabled = false });
                return;
            }

            foreach (var parent in parents)
                item.Items.Add(CreateAncestorTreeItem(tag.TreeId, parent));
        }
        catch (Exception ex)
        {
            item.Items.Clear();
            item.Items.Add(new TreeViewItem { Header = $"加载失败: {ex.Message}", IsEnabled = false });
            tag.ChildrenLoaded = false;
            item.Expanded += LazyAncestorItem_Expanded;
        }
    }

    private void CreateClanCancelButton_Click(object sender, RoutedEventArgs e)
    {
        CreateTreeNameInput.Text = "";
    }

    private void InviteButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is Button { DataContext: ClanInfo clan })
        {
            _currentTreeId = ulong.Parse(clan.Id);
            ApplyCurrentTreeIdToQueryInputs();
            InviteUserIdInput.Text = "";
            OpenCenteredPopup(InvitePopup);
        }
        else
        {
            MessageBox.Show("请先选择要邀请管理的族谱。", "邀请管理", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }

    private void MemberOperationButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is Button button && button.DataContext is ClanInfo clan)
        {
            _currentTreeId = ulong.Parse(clan.Id);
            ApplyCurrentTreeIdToQueryInputs();
            OpenCenteredPopup(MemberPopup);
        }
    }

    private static readonly IntPtr HwndTopMost = new(-1);
    private const uint SwpNomove = 0x0002;
    private const uint SwpNosize = 0x0001;
    private const uint SwpNoActivate = 0x0010;

    [DllImport("user32.dll", SetLastError = true)]
    private static extern bool SetWindowPos(IntPtr hWnd, IntPtr hWndInsertAfter, int x, int y, int cx, int cy, uint uFlags);

    private void OpenCenteredPopup(Popup popup)
    {
        void OnOpened(object? s, EventArgs e)
        {
            popup.Opened -= OnOpened;
            if (popup.Child != null)
                popup.Dispatcher.BeginInvoke(() => SetPopupTopmost(popup), DispatcherPriority.Loaded);
        }

        popup.Opened -= OnOpened;
        popup.Opened += OnOpened;
        popup.PlacementTarget = PageRoot;
        popup.Placement = PlacementMode.Center;
        popup.HorizontalOffset = 0;
        popup.VerticalOffset = 0;
        popup.IsOpen = true;
    }

    private static void SetPopupTopmost(Popup popup)
    {
        if (popup.Child is not Visual visual)
            return;
        if (PresentationSource.FromVisual(visual) is HwndSource { Handle: var handle } && handle != IntPtr.Zero)
            SetWindowPos(handle, HwndTopMost, 0, 0, 0, 0, SwpNomove | SwpNosize | SwpNoActivate);
    }

    private void MemberPopupClose_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
        MemberInfoPanel.Children.Clear();
    }

    private void AddMember_Click(object sender, RoutedEventArgs e)
    {
        MemberInfoPanel.Children.Clear();
        BuildAddMemberForm();
    }

    private void ModifyMember_Click(object sender, RoutedEventArgs e)
    {
        MemberInfoPanel.Children.Clear();
        BuildModifyMemberForm();
    }

    private void DeleteMember_Click(object sender, RoutedEventArgs e)
    {
        MemberInfoPanel.Children.Clear();
        BuildDeleteMemberForm();
    }

    private void DeleteButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is Button { DataContext: ClanInfo clan })
        {
            _currentTreeId = ulong.Parse(clan.Id);
            ApplyCurrentTreeIdToQueryInputs();
            OpenCenteredPopup(DeleteConfirmPopup);
        }
        else
        {
            MessageBox.Show("请先选择要删除的族谱。", "删除族谱", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }

    private void BuildAddMemberForm()
    {
        var stackPanel = new StackPanel { Margin = new Thickness(0, 10, 0, 0) };

        // FullName
        stackPanel.Children.Add(new TextBlock { Text = "姓名", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        var fullNameBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(fullNameBox);

        // Gender
        stackPanel.Children.Add(new TextBlock { Text = "性别", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        var genderCombo = new ComboBox { Margin = new Thickness(0, 0, 0, 10) };
        genderCombo.Items.Add("M");
        genderCombo.Items.Add("F");
        genderCombo.SelectedIndex = 0;
        stackPanel.Children.Add(genderCombo);

        // BirthDate
        stackPanel.Children.Add(new TextBlock { Text = "出生日期", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        var birthDatePicker = new DatePicker { Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(birthDatePicker);

        // DeathDate
        stackPanel.Children.Add(new TextBlock { Text = "死亡日期", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        var deathDatePicker = new DatePicker { Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(deathDatePicker);

        // Biography
        stackPanel.Children.Add(new TextBlock { Text = "传记", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        var biographyBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10), AcceptsReturn = true, Height = 60 };
        stackPanel.Children.Add(biographyBox);

        // FatherMemberId
        stackPanel.Children.Add(new TextBlock { Text = "父亲成员ID (可选)", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        var fatherIdBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(fatherIdBox);

        // MotherMemberId
        stackPanel.Children.Add(new TextBlock { Text = "母亲成员ID (可选)", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        var motherIdBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(motherIdBox);

        // Generation
        stackPanel.Children.Add(new TextBlock { Text = "世代 (可选)", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        var generationBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(generationBox);

        // Submit Button
        var submitButton = new Button { Content = "添加成员", Style = (Style)FindResource("OperationButtonStyle"), Margin = new Thickness(0, 10, 0, 0) };
        submitButton.Click += async (s, e) =>
        {
            var request = new MemberCreateRequest
            {
                TreeId = _currentTreeId,
                FullName = fullNameBox.Text,
                Gender = genderCombo.SelectedItem.ToString()!,
                BirthDate = birthDatePicker.SelectedDate,
                DeathDate = deathDatePicker.SelectedDate,
                Biography = string.IsNullOrWhiteSpace(biographyBox.Text) ? null : biographyBox.Text,
                FatherMemberId = string.IsNullOrWhiteSpace(fatherIdBox.Text) ? null : ulong.Parse(fatherIdBox.Text),
                MotherMemberId = string.IsNullOrWhiteSpace(motherIdBox.Text) ? null : ulong.Parse(motherIdBox.Text),
                Generation = string.IsNullOrWhiteSpace(generationBox.Text) ? null : uint.Parse(generationBox.Text)
            };

            try
            {
                var response = await _httpClient.PostAsJsonAsync("api/Members", request);
                var body = await response.Content.ReadAsStringAsync();

                if (TryParseActionResponse(response, body, out var error))
                {
                    MessageBox.Show("成员添加成功", "成功", MessageBoxButton.OK, MessageBoxImage.Information);
                    MemberPopup.IsOpen = false;
                }
                else
                {
                    MessageBox.Show(error ?? "添加失败", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"添加失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        };
        stackPanel.Children.Add(submitButton);

        MemberInfoPanel.Children.Add(stackPanel);
    }

    private void BuildModifyMemberForm()
    {
        var stackPanel = new StackPanel { Margin = new Thickness(0, 10, 0, 0) };

        stackPanel.Children.Add(new TextBlock
        {
            Text = "1. 选择成员  2. 修改下方信息  3. 点击「确认修改完成」",
            FontSize = 13,
            Foreground = new SolidColorBrush(Color.FromRgb(138, 126, 114)),
            Margin = new Thickness(0, 0, 0, 10)
        });

        var membersList = new ListBox { Height = 140, Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(new TextBlock { Text = "选择要修改的成员", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        stackPanel.Children.Add(membersList);
        _ = LoadMembersIntoListAsync(membersList);

        var formPanel = new StackPanel { Visibility = Visibility.Collapsed };

        var fullNameBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10) };
        var genderCombo = new ComboBox { Margin = new Thickness(0, 0, 0, 10) };
        genderCombo.Items.Add("M");
        genderCombo.Items.Add("F");
        var birthDatePicker = new DatePicker { Margin = new Thickness(0, 0, 0, 10) };
        var deathDatePicker = new DatePicker { Margin = new Thickness(0, 0, 0, 10) };
        var biographyBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10), AcceptsReturn = true, Height = 60 };
        var fatherIdBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10) };
        var motherIdBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10) };
        var generationBox = new TextBox { Style = (Style)FindResource("InputBoxStyle"), Margin = new Thickness(0, 0, 0, 10) };

        formPanel.Children.Add(new TextBlock { Text = "姓名", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(fullNameBox);
        formPanel.Children.Add(new TextBlock { Text = "性别 (M/F)", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(genderCombo);
        formPanel.Children.Add(new TextBlock { Text = "出生日期", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(birthDatePicker);
        formPanel.Children.Add(new TextBlock { Text = "死亡日期", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(deathDatePicker);
        formPanel.Children.Add(new TextBlock { Text = "传记", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(biographyBox);
        formPanel.Children.Add(new TextBlock { Text = "父亲成员ID (可选)", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(fatherIdBox);
        formPanel.Children.Add(new TextBlock { Text = "母亲成员ID (可选)", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(motherIdBox);
        formPanel.Children.Add(new TextBlock { Text = "世代 (可选)", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(generationBox);

        stackPanel.Children.Add(formPanel);

        var actionRow = new StackPanel
        {
            Orientation = Orientation.Horizontal,
            Margin = new Thickness(0, 16, 0, 0),
            Visibility = Visibility.Collapsed
        };

        var confirmButton = new Button
        {
            Content = "确认修改完成",
            Style = (Style)FindResource("OperationButtonStyle"),
            Width = 120,
            IsEnabled = false
        };
        var cancelButton = new Button
        {
            Content = "取消",
            Style = (Style)FindResource("OperationButtonStyle"),
            Margin = new Thickness(16, 0, 0, 0),
            Width = 80
        };
        actionRow.Children.Add(confirmButton);
        actionRow.Children.Add(cancelButton);
        stackPanel.Children.Add(actionRow);

        static void SelectGender(ComboBox combo, string gender) =>
            combo.SelectedIndex = gender.Equals("F", StringComparison.OrdinalIgnoreCase) ? 1 : 0;

        membersList.SelectionChanged += (_, _) =>
        {
            if (membersList.SelectedItem is not MemberDto member)
            {
                formPanel.Visibility = Visibility.Collapsed;
                actionRow.Visibility = Visibility.Collapsed;
                confirmButton.IsEnabled = false;
                return;
            }

            fullNameBox.Text = member.FullName;
            SelectGender(genderCombo, member.Gender);
            birthDatePicker.SelectedDate = member.BirthDate;
            deathDatePicker.SelectedDate = member.DeathDate;
            biographyBox.Text = member.Biography ?? "";
            fatherIdBox.Text = member.FatherMemberId?.ToString() ?? "";
            motherIdBox.Text = member.MotherMemberId?.ToString() ?? "";
            generationBox.Text = member.Generation?.ToString() ?? "";
            formPanel.Visibility = Visibility.Visible;
            actionRow.Visibility = Visibility.Visible;
            confirmButton.IsEnabled = true;
        };

        cancelButton.Click += (_, _) => MemberPopupClose_Click(cancelButton, new RoutedEventArgs());

        confirmButton.Click += async (_, _) =>
        {
            if (membersList.SelectedItem is not MemberDto member)
            {
                MessageBox.Show("请先选择要修改的成员。", "修改信息", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            if (string.IsNullOrWhiteSpace(fullNameBox.Text))
            {
                MessageBox.Show("姓名不能为空。", "修改信息", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            if (genderCombo.SelectedItem is not string gender)
            {
                MessageBox.Show("请选择性别。", "修改信息", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            ulong? fatherId = null;
            ulong? motherId = null;
            uint? generation = null;
            try
            {
                if (!string.IsNullOrWhiteSpace(fatherIdBox.Text))
                    fatherId = ulong.Parse(fatherIdBox.Text.Trim());
                if (!string.IsNullOrWhiteSpace(motherIdBox.Text))
                    motherId = ulong.Parse(motherIdBox.Text.Trim());
                if (!string.IsNullOrWhiteSpace(generationBox.Text))
                    generation = uint.Parse(generationBox.Text.Trim());
            }
            catch (FormatException)
            {
                MessageBox.Show("父亲/母亲 ID 或世代必须是数字。", "修改信息", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            var request = new MemberUpdateRequest
            {
                FullName = fullNameBox.Text.Trim(),
                Gender = gender,
                BirthDate = birthDatePicker.SelectedDate,
                DeathDate = deathDatePicker.SelectedDate,
                Biography = string.IsNullOrWhiteSpace(biographyBox.Text) ? null : biographyBox.Text.Trim(),
                FatherMemberId = fatherId,
                MotherMemberId = motherId,
                Generation = generation
            };

            try
            {
                confirmButton.IsEnabled = false;
                var response = await _httpClient.PutAsJsonAsync($"api/Members/{_currentTreeId}/{member.MemberId}", request);
                var body = await response.Content.ReadAsStringAsync();

                if (TryParseActionResponse(response, body, out var error))
                {
                    MessageBox.Show("成员信息已保存。", "修改完成", MessageBoxButton.OK, MessageBoxImage.Information);
                    await LoadMembersIntoListAsync(membersList);
                }
                else
                {
                    MessageBox.Show(error ?? "更新失败", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"更新失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                confirmButton.IsEnabled = membersList.SelectedItem != null;
            }
        };

        MemberInfoPanel.Children.Add(stackPanel);
    }

    private void BuildDeleteMemberForm()
    {
        var stackPanel = new StackPanel { Margin = new Thickness(0, 10, 0, 0) };

        // Load members list
        var membersList = new ListBox { Height = 200, Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(new TextBlock { Text = "选择要删除的成员", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        stackPanel.Children.Add(membersList);

        // Load members
        _ = LoadMembersIntoListAsync(membersList);

        var deleteButton = new Button { Content = "删除成员", Style = (Style)FindResource("OperationButtonStyle"), Margin = new Thickness(0, 10, 0, 0), IsEnabled = false };
        stackPanel.Children.Add(deleteButton);

        // Handle selection
        membersList.SelectionChanged += (s, e) =>
        {
            deleteButton.IsEnabled = membersList.SelectedItem != null;
        };

        // Delete button click
        deleteButton.Click += async (s, e) =>
        {
            if (membersList.SelectedItem is not MemberDto member) return;

            var result = MessageBox.Show($"确定要删除成员 {member.FullName} 吗？", "确认删除", MessageBoxButton.YesNo, MessageBoxImage.Warning);
            if (result != MessageBoxResult.Yes) return;

            try
            {
                var response = await _httpClient.DeleteAsync($"api/Members/{_currentTreeId}/{member.MemberId}");
                var body = await response.Content.ReadAsStringAsync();

                if (TryParseActionResponse(response, body, out var error))
                {
                    MessageBox.Show("成员删除成功", "成功", MessageBoxButton.OK, MessageBoxImage.Information);
                    await LoadMembersIntoListAsync(membersList);
                }
                else
                {
                    MessageBox.Show(error ?? "删除失败", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"删除失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        };

        MemberInfoPanel.Children.Add(stackPanel);
    }

    private async void ClanDeleteConfirm_Click(object sender, RoutedEventArgs e)
    {
        DeleteConfirmPopup.IsOpen = false;

        if (_currentTreeId == 0)
        {
            MessageBox.Show("未选择族谱。", "删除族谱", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        try
        {
            var response = await _httpClient.DeleteAsync($"api/familytrees/{_currentTreeId}?userId={_userId}");
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<FamilyTreeInviteResponseDto>(body, JsonOptions);

            if (TryParseActionResponse(response, body, out var error))
            {
                MessageBox.Show(result?.Message ?? "族谱已删除", "删除族谱", MessageBoxButton.OK, MessageBoxImage.Information);
                _currentTreeId = 0;
                await LoadUserFamilyTreesAsync();
            }
            else
            {
                MessageBox.Show(error ?? result?.Message ?? "删除失败", "删除族谱",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"删除失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void ClanDeleteCancel_Click(object sender, RoutedEventArgs e)
    {
        DeleteConfirmPopup.IsOpen = false;
    }

    private async void InviteConfirmButton_Click(object sender, RoutedEventArgs e)
    {
        var idText = InviteUserIdInput.GetActualText().Trim();
        if (!ulong.TryParse(idText, out var inviteeUserId) || inviteeUserId == 0)
        {
            MessageBox.Show("请输入有效的被邀请人用户 ID（数字）。", "邀请管理", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        if (_currentTreeId == 0)
        {
            MessageBox.Show("未选择族谱，请从「我创建的族谱」列表点击「邀请管理」。", "邀请管理", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        try
        {
            var response = await _httpClient.PostAsJsonAsync(
                $"api/familytrees/{_currentTreeId}/invite?ownerUserId={_userId}",
                new { inviteeUserId });
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<FamilyTreeInviteResponseDto>(body, JsonOptions);

            if (result is { Success: true })
            {
                MessageBox.Show(result.Message ?? "邀请成功", "邀请管理", MessageBoxButton.OK, MessageBoxImage.Information);
                InvitePopup.IsOpen = false;
                InviteUserIdInput.Text = "";
            }
            else
            {
                MessageBox.Show(result?.Message ?? $"邀请失败（HTTP {(int)response.StatusCode}）", "邀请管理",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"邀请失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void InviteCancelButton_Click(object sender, RoutedEventArgs e)
    {
        InvitePopup.IsOpen = false;
    }

    private void AddMemberConfirmButton_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
    }

    private void AddMemberCancelButton_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
    }







    private void ModifyMemberConfirmButton_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
    }

    private void ModifyMemberCancelButton_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
    }

    private void DeleteMemberConfirmButton_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
    }

    private void DeleteMemberCancelButton_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
    }
}
