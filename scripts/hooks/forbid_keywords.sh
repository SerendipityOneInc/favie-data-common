#!/bin/sh

# 获取脚本自身的文件路径
script_name=$(basename "$0")
script_path=$(dirname "$0")/$script_name

# 检查文件和目录名是否包含 shoppal 或 Shoppal
for file in $(git ls-files); do
  if [[ "$file" =~ shoppal|Shoppal ]] && [[ "$file" != "$script_path" ]]; then
    echo "Error: File or directory name '$file' contains forbidden keyword 'shoppal' or 'Shoppal'"
    exit 1
  fi
done

# 检查文件内容是否包含 shoppal 或 Shoppal，但排除自身
for file in $(git ls-files); do
  if [[ "$file" != "$script_path" ]] && grep -qE 'shoppal|Shoppal' "$file"; then
    echo "Error: File '$file' contains forbidden keyword 'shoppal' or 'Shoppal'"
    exit 1
  fi
done

exit 0