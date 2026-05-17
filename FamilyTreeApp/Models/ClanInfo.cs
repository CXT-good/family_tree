using System;

namespace FamilyTreeApp.Models
{
    public class ClanInfo
    {
        // 基本信息
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
        public string Creator { get; set; } = string.Empty;
        public string LastModified { get; set; } = string.Empty;

        // 成员统计
        public int TotalMembers { get; set; }
        public int MaleCount { get; set; }
        public int FemaleCount { get; set; }

        // 创建信息
        public string CreateDate { get; set; } = string.Empty;

        // 构造函数
        public ClanInfo()
        {
        }

        // 带参数的构造函数
        public ClanInfo(string id, string name, string surname, string creator,
                      string lastModified, int totalMembers, int maleCount,
                      int femaleCount, string createDate)
        {
            Id = id;
            Name = name;
            Surname = surname;
            Creator = creator;
            LastModified = lastModified;
            TotalMembers = totalMembers;
            MaleCount = maleCount;
            FemaleCount = femaleCount;
            CreateDate = createDate;
        }

        // 辅助方法
        public double MalePercentage => TotalMembers > 0 ? (double)MaleCount / TotalMembers * 100 : 0;
        public double FemalePercentage => TotalMembers > 0 ? (double)FemaleCount / TotalMembers * 100 : 0;
    }
}
