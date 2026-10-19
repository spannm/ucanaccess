#!/usr/bin/env bash
#
# Regenerates the Maven site and copies it into docs/, which is served by GitHub Pages.
# Review the result with git diff and commit it separately as "docs: update site".
#
# Notes:
# - 'package' must run before 'site': only with the built jar (Automatic-Module-Name)
#   does javadoc produce the module layout (apidocs/io.github.spannm.ucanaccess/...) used in docs/.
# - docs/ is updated without deleting files, because some files exist only there
#   (e.g. docs/images/ucanaccess-logo.svg, used by README.md).
# - The xref report is written with CRLF line endings. With core.safecrlf=true git refuses
#   to stage such files, so carriage returns are stripped after copying.

set -euo pipefail

cd "$(dirname "$0")/.."

./mvnw -B clean package site -DskipTests

rsync -a --checksum --exclude jacoco target/site/ docs/

grep -rlZ $'\r' docs --include='*.html' --include='*.css' --include='*.js' | xargs -0 -r sed -i 's/\r$//'

git status --short docs
