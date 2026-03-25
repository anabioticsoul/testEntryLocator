import csv
import re
from urllib.parse import urlparse, urlunparse


def extract_repo_root_url(url):
    """
    从GitHub/GitLab文件URL中提取仓库根目录URL
    例如：
    输入: https://github.com/cloud-ark/caastle/blob/.../file.yaml
    输出: https://github.com/cloud-ark/caastle

    输入: https://gitlab.com/ska-telescope/skampi/blob/.../file.yaml
    输出: https://gitlab.com/ska-telescope/skampi
    """
    parsed = urlparse(url)
    path_parts = parsed.path.strip('/').split('/')

    if len(path_parts) >= 2:
        # 获取仓库所有者和仓库名
        owner = path_parts[0]
        repo = path_parts[1]

        # 构建仓库根URL
        root_path = f"/{owner}/{repo}"
        root_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            root_path,
            '',  # params
            '',  # query
            ''  # fragment
        ))
        return root_url

    return url  # 如果无法解析，返回原URL


def process_csv_to_repo_urls(input_file, output_file):
    """
    处理CSV文件，提取每个URL对应的仓库根URL
    """
    repo_urls = set()  # 使用set去重

    # 读取输入文件
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)

        for row in reader:
            if row:
                url = row[0].strip()
                if url:
                    # 提取仓库根URL
                    repo_root_url = extract_repo_root_url(url)
                    if repo_root_url:
                        repo_urls.add(repo_root_url)

    # 写入输出文件
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)

        # 每个仓库URL单独一行
        for repo_url in sorted(repo_urls):
            writer.writerow([repo_url])

    # 统计信息
    print(f"处理完成！")
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print(f"发现的仓库数量: {len(repo_urls)}")

    # 显示一些示例
    print("\n示例仓库URL:")
    for i, repo_url in enumerate(sorted(repo_urls)[:5]):
        print(f"  {i + 1}. {repo_url}")


def main():
    input_file = '../GITlab-URLS.csv'
    output_file = 'repo_gitlab_urls.csv'

    process_csv_to_repo_urls(input_file, output_file)


if __name__ == "__main__":
    main()