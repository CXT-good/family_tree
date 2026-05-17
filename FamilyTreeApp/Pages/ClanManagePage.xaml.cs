using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Threading;
using FamilyTreeApp.Controls;
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
            BranchTreeView.Items.Clear();
            var response = await _httpClient.GetAsync(
                $"api/Members/branch?treeId={treeId}&rootMemberId={rootMemberId}&maxDepth=4");
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<MemberTreeResponseDto>(body, JsonOptions);

            if (result is not { Success: true, Data: { } node })
            {
                MessageBox.Show(result?.Message ?? "未找到该分支或查询失败。", "分支预览", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            BranchTreeView.Items.Add(CreateTreeViewItem(node, expandAll: true));
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
            AncestorTreeView.Items.Clear();
            var response = await _httpClient.GetAsync($"api/Members/ancestors?treeId={treeId}&memberId={memberId}");
            var body = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<MemberTreeResponseDto>(body, JsonOptions);

            if (result is not { Success: true, Data: { } node })
            {
                MessageBox.Show(result?.Message ?? "未找到该成员或查询失败。", "祖先查询", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            AncestorTreeView.Items.Add(CreateTreeViewItem(node, expandAll: true));
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

    private static TreeViewItem CreateTreeViewItem(MemberTreeNodeDto node, bool expandAll = false)
    {
        var header = string.IsNullOrWhiteSpace(node.Relation)
            ? node.FullName
            : $"{node.Relation}: {node.FullName}";

        if (node.Generation.HasValue)
            header += $" · 世代 {node.Generation}";
        if (!string.IsNullOrWhiteSpace(node.Gender))
            header += $" · {node.Gender}";

        header += $" (ID: {node.MemberId})";

        var item = new TreeViewItem { Header = header };
        foreach (var child in node.Children ?? [])
        {
            var childItem = CreateTreeViewItem(child, expandAll);
            item.Items.Add(childItem);
        }

        if (expandAll && item.Items.Count > 0)
            item.IsExpanded = true;

        return item;
    }

    private void CreateClanCancelButton_Click(object sender, RoutedEventArgs e)
    {
        CreateTreeNameInput.Text = "";
    }

    private void InviteButton_Click(object sender, RoutedEventArgs e)
    {
        InvitePopup.IsOpen = true;
    }

    private void MemberOperationButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is Button button && button.DataContext is ClanInfo clan)
        {
            _currentTreeId = ulong.Parse(clan.Id);
            MemberPopup.IsOpen = true;
        }
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
        DeleteConfirmPopup.IsOpen = true;
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
                var result = JsonSerializer.Deserialize<Dictionary<string, object>>(body, JsonOptions);

                if (result != null && result.ContainsKey("success") && (bool)result["success"])
                {
                    MessageBox.Show("成员添加成功", "成功", MessageBoxButton.OK, MessageBoxImage.Information);
                    MemberPopup.IsOpen = false;
                }
                else
                {
                    MessageBox.Show(result?["message"]?.ToString() ?? "添加失败", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
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

        // Load members list
        var membersList = new ListBox { Height = 200, Margin = new Thickness(0, 0, 0, 10) };
        stackPanel.Children.Add(new TextBlock { Text = "选择要修改的成员", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        stackPanel.Children.Add(membersList);

        // Load members
        Task.Run(async () =>
        {
            try
            {
                var response = await _httpClient.GetAsync($"api/Members?treeId={_currentTreeId}&page=1&pageSize=100");
                var body = await response.Content.ReadAsStringAsync();
                var result = JsonSerializer.Deserialize<MemberListResponseDto>(body, JsonOptions);

                if (result != null && result.Success)
                {
                    Dispatcher.Invoke(() =>
                    {
                        membersList.ItemsSource = result.Items;
                        membersList.DisplayMemberPath = "FullName";
                    });
                }
            }
            catch (Exception ex)
            {
                Dispatcher.Invoke(() => MessageBox.Show($"加载成员失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error));
            }
        });

        // Form fields (initially hidden)
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
        formPanel.Children.Add(new TextBlock { Text = "性别", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(genderCombo);
        formPanel.Children.Add(new TextBlock { Text = "出生日期", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(birthDatePicker);
        formPanel.Children.Add(new TextBlock { Text = "死亡日期", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(deathDatePicker);
        formPanel.Children.Add(new TextBlock { Text = "传记", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(biographyBox);
        formPanel.Children.Add(new TextBlock { Text = "父亲成员ID", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(fatherIdBox);
        formPanel.Children.Add(new TextBlock { Text = "母亲成员ID", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(motherIdBox);
        formPanel.Children.Add(new TextBlock { Text = "世代", FontSize = 14, Margin = new Thickness(0, 0, 0, 5) });
        formPanel.Children.Add(generationBox);

        var updateButton = new Button { Content = "更新成员", Style = (Style)FindResource("OperationButtonStyle"), Margin = new Thickness(0, 10, 0, 0) };
        formPanel.Children.Add(updateButton);

        stackPanel.Children.Add(formPanel);

        // Handle selection
        membersList.SelectionChanged += (s, e) =>
        {
            if (membersList.SelectedItem is MemberDto member)
            {
                fullNameBox.Text = member.FullName;
                genderCombo.SelectedItem = member.Gender;
                birthDatePicker.SelectedDate = member.BirthDate;
                deathDatePicker.SelectedDate = member.DeathDate;
                biographyBox.Text = member.Biography ?? "";
                fatherIdBox.Text = member.FatherMemberId?.ToString() ?? "";
                motherIdBox.Text = member.MotherMemberId?.ToString() ?? "";
                generationBox.Text = member.Generation?.ToString() ?? "";
                formPanel.Visibility = Visibility.Visible;
            }
        };

        // Update button click
        updateButton.Click += async (s, e) =>
        {
            if (membersList.SelectedItem is not MemberDto member) return;

            var request = new MemberUpdateRequest
            {
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
                var response = await _httpClient.PutAsJsonAsync($"api/Members/{_currentTreeId}/{member.MemberId}", request);
                var body = await response.Content.ReadAsStringAsync();
                var result = JsonSerializer.Deserialize<Dictionary<string, object>>(body, JsonOptions);

                if (result != null && result.ContainsKey("success") && (bool)result["success"])
                {
                    MessageBox.Show("成员更新成功", "成功", MessageBoxButton.OK, MessageBoxImage.Information);
                    MemberPopup.IsOpen = false;
                }
                else
                {
                    MessageBox.Show(result?["message"]?.ToString() ?? "更新失败", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"更新失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
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
        Task.Run(async () =>
        {
            try
            {
                var response = await _httpClient.GetAsync($"api/Members?treeId={_currentTreeId}&page=1&pageSize=100");
                var body = await response.Content.ReadAsStringAsync();
                var result = JsonSerializer.Deserialize<MemberListResponseDto>(body, JsonOptions);

                if (result != null && result.Success)
                {
                    Dispatcher.Invoke(() =>
                    {
                        membersList.ItemsSource = result.Items;
                        membersList.DisplayMemberPath = "FullName";
                    });
                }
            }
            catch (Exception ex)
            {
                Dispatcher.Invoke(() => MessageBox.Show($"加载成员失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error));
            }
        });

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
                var responseResult = JsonSerializer.Deserialize<Dictionary<string, object>>(body, JsonOptions);

                if (responseResult != null && responseResult.ContainsKey("success") && (bool)responseResult["success"])
                {
                    MessageBox.Show("成员删除成功", "成功", MessageBoxButton.OK, MessageBoxImage.Information);
                    MemberPopup.IsOpen = false;
                }
                else
                {
                    MessageBox.Show(responseResult?["message"]?.ToString() ?? "删除失败", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"删除失败: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        };

        MemberInfoPanel.Children.Add(stackPanel);
    }

    private void MemberToolbarDeleteConfirm_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
    }

    private void MemberToolbarDeleteCancel_Click(object sender, RoutedEventArgs e)
    {
        MemberPopup.IsOpen = false;
    }

    private void ClanDeleteConfirm_Click(object sender, RoutedEventArgs e)
    {
        DeleteConfirmPopup.IsOpen = false;
        MessageBox.Show("删除族谱接口尚未接入后端。", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
    }

    private void ClanDeleteCancel_Click(object sender, RoutedEventArgs e)
    {
        DeleteConfirmPopup.IsOpen = false;
    }

    private void InviteConfirmButton_Click(object sender, RoutedEventArgs e)
    {
        InvitePopup.IsOpen = false;
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
