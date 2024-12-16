#!/bin/bash

poetry lock
poetry install

git add .
git commit -m 'Update peotry.lock'