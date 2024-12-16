#!/bin/bash

if git diff --cached --name-only | grep -q 'pyproject\.toml'; then
    echo "pyproject.toml has changed. Updating lockfile and installing dependencies."
    poetry lock
    poetry install

    git add .
    git commit -m "Install dependencies using poetry"
fi