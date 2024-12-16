#!/bin/sh

# 检查是否已经安装 gh 命令
if ! command -v gh &> /dev/null; then
  echo "Error: gh (GitHub CLI) is not installed."
  exit 1
fi

# 获取当前 GitHub 用户信息
USER_INFO=$(gh api user)

# 提取 user.name 和 user.email
USER_NAME=$(echo "$USER_INFO" | jq -r '.login')
USER_EMAIL=$(echo "$USER_INFO" | jq -r '.email')

# 检查是否成功获取 user.name 和 user.email
if [ -z "$USER_NAME" ] || [ "$USER_NAME" == "null" ]; then
  echo "Error: Could not retrieve user.name from GitHub."
  exit 1
fi

if [ -z "$USER_EMAIL" ] || [ "$USER_EMAIL" == "null" ]; then
  echo "Error: Could not retrieve user.email from GitHub."
  exit 1
fi

# 检查 user.name 是否以 -srp 结尾
if [[ ! $USER_NAME =~ -srp$ ]]; then
  echo "Error: user.name must end with -srp"
  exit 1
fi

# 检查 user.email 是否以 @srp.one 结尾
if [[ ! $USER_EMAIL =~ @srp.one$ ]]; then
  echo "Error: user.email must end with @srp.one"
  exit 1
fi

# 设置 Git 配置中的 user.name 和 user.email
git config --global user.name "$USER_NAME"
git config --global user.email "$USER_EMAIL"

echo "user.name: $USER_NAME, user.email: $USER_EMAIL"
echo "user.name and user.email are compliant."
exit 0